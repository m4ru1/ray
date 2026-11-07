"""Derive ``ListFiles`` pushdown state from its consuming ``ReadFiles`` scanner.

This is the single source of truth for the read constraints a ``ListFiles``
applies while listing. It runs once, after every other logical rule, so no rule
has to remember to keep the two operators in sync.
"""

from dataclasses import replace

from ray.data._internal.datasource_v2.common.listing_pruning import ListingPruningHint
from ray.data._internal.datasource_v2.common.pushdown_utils import (
    derive_list_files_pushdown,
)
from ray.data._internal.datasource_v2.formats.parquet.footer_file_indexer import (
    FooterFileIndexer,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_scanner import (
    ParquetScanner,
)
from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan, Rule
from ray.data._internal.logical.operators.map_operator import Filter
from ray.data._internal.logical.operators.read_operator import ListFiles, ReadFiles

__all__ = [
    "DeriveListFilesPushdown",
]


class DeriveListFilesPushdown(Rule):
    """Sync each ``ListFiles``' listing constraints from its consuming scanner.

    Listing-time prune (predicate / projected columns / limit) is only sound if
    it is no stronger than what the downstream ``ReadFiles`` applies. This rule
    derives that state from the scanner once, after the optimize loop, instead
    of having every pushdown rule mirror onto ``ListFiles``. Non-``ReadFiles``
    consumers (e.g. after ``PushdownCountFiles``) clear the constraints.
    A separate residual hint is backed by the retained Filter, never exact count.

    The limit is never derived onto a ``ListFiles`` that shuffles row groups
    (``FileShuffleConfig.small_chunks_shuffle``): the shuffle runs on the listed
    rows, so an early stop would shrink the shuffle pool. The scanner keeps its
    limit; it applies after the shuffle.
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:  # pyrefly: ignore[bad-override]
        def transform(node: LogicalOperator) -> LogicalOperator:
            inputs = node.input_dependencies
            if not any(isinstance(input_op, ListFiles) for input_op in inputs):
                return node

            # ``ReadFiles`` is the only consumer that applies these constraints
            # downstream; for anything else they must be dropped.
            scanner = node.scanner if isinstance(node, ReadFiles) else None
            pushdown = derive_list_files_pushdown(scanner)

            new_inputs: list[LogicalOperator] = []
            changed = False
            for input_op in inputs:
                if not isinstance(input_op, ListFiles):
                    new_inputs.append(input_op)
                    continue
                # Listing stops at the limit *before* the row-group shuffle, so
                # the shuffle would only permute the first files listed.
                limit = None if _shuffles_row_groups(input_op) else pushdown.limit
                if (
                    input_op.predicate is not pushdown.predicate
                    or input_op.projected_columns != pushdown.projected_columns
                    or input_op.limit != limit
                    # Identity like ``predicate``: ``Expr.__eq__`` builds an
                    # expression, not a bool. Rebuilt per call, so this is
                    # unequal whenever a pruner exists.
                    or input_op.partition_pruner is not pushdown.partition_pruner
                ):
                    # Spelled out rather than ``**asdict(pushdown)``: that
                    # deep-copies, which would clone the pruner on every pass.
                    input_op = replace(
                        input_op,
                        predicate=pushdown.predicate,
                        projected_columns=pushdown.projected_columns,
                        limit=limit,
                        partition_pruner=pushdown.partition_pruner,
                    )
                    changed = True
                new_inputs.append(input_op)

            if not changed:
                return node
            return node._with_new_input_dependencies(new_inputs)

        def clear_listing_hint(node: LogicalOperator) -> LogicalOperator:
            if not isinstance(node, ListFiles):
                return node
            # Only the first, explicitly admitted built-in adapter owns hints.
            # Do not call a new hook on arbitrary indexers or their subclasses.
            if type(node.file_indexer) is not FooterFileIndexer:
                return node
            indexer = node.file_indexer.with_listing_hint(None)
            if indexer is node.file_indexer:
                return node
            return replace(node, file_indexer=indexer)

        # Clear every source before any future eligibility early return. This
        # includes bare ListFiles and sources whose ReadFiles disappeared.
        # Clearing clones capable indexers, never shared configurations.
        dag = plan.dag._apply_transform(clear_listing_hint)
        dag = dag._apply_transform(transform)

        # Ordinary row queries first. This is a structural scope restriction,
        # not a complete-consumption/error-preservation gate. Other roots decline.
        if (
            type(dag) is Filter
            and len(dag.input_dependencies) == 1
            and not plan.context.execution_options.preserve_order
            and plan.context.max_errored_blocks == 0
            and plan.context.checkpoint_config is None
        ):
            read = dag.input_dependencies[0]
            if (
                type(read) is ReadFiles
                and read.block_udf is None
                and type(read.scanner) is ParquetScanner
                and read.scanner.limit is None
                and len(read.input_dependencies) == 1
            ):
                listing = read.input_dependencies[0]
                if (
                    type(listing) is ListFiles
                    and type(listing.file_indexer) is FooterFileIndexer
                    and listing.limit is None
                    and listing.shuffle_config_factory() is None
                ):
                    context = read.scanner.listing_pruning_context()
                    residual = dag.predicate_expr
                    hint = (
                        ListingPruningHint.from_residual(context, residual)
                        if context is not None and residual is not None
                        else None
                    )
                    if hint is not None:
                        configured = replace(
                            listing,
                            file_indexer=listing.file_indexer.with_listing_hint(hint),
                        )
                        read = read._with_new_input_dependencies([configured])
                        dag = dag._with_new_input_dependencies([read])
        return LogicalPlan(dag=dag, context=plan.context)


def _shuffles_row_groups(list_files: ListFiles) -> bool:
    shuffle_config = list_files.shuffle_config_factory()
    return shuffle_config is not None and shuffle_config.small_chunks_shuffle
