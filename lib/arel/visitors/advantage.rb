module Arel
  module Visitors
    class Advantage < Arel::Visitors::ToSql
      private

      if Rails::VERSION::MAJOR >= 4
        # Rails 4 or above
        def visit_Arel_Nodes_Offset(o, collector)
          collector << "START AT "
          visit (o.expr + 1), collector
          collector << " "
          collector
        end

        def visit_Arel_Nodes_Limit(o, collector)
          collector << "TOP  "
          visit o.expr, collector
          collector << " "
          collector
        end

        # copied from informix.rb
        def visit_Arel_Nodes_SelectStatement(o, collector)
          collector << "SELECT "
          collector = maybe_visit o.limit, collector
          collector = maybe_visit o.offset, collector
          collector = o.cores.inject(collector) { |c, x|
            visit_Arel_Nodes_SelectCore x, c
          }
          if o.orders.any?
            collector << "ORDER BY "
            collector = inject_join o.orders, collector, ", "
          end
          collector
        end

        def visit_Arel_Nodes_SelectCore(o, collector)
          collector = inject_join o.projections, collector, ", "
          if o.source && !o.source.empty?
            collector << " FROM "
            collector = visit o.source, collector
          end

          if o.wheres.any?
            collector << " WHERE "
            collector = inject_join o.wheres, collector, " AND "
          end

          if o.groups.any?
            collector << "GROUP BY "
            collector = inject_join o.groups, collector, ", "
          end

          collector
        end
      else
        # Rails 3 or lower
        def visit_Arel_Nodes_Offset(o)
          "START AT #{visit(o.expr) + 1}"
        end

        def visit_Arel_Nodes_Limit(o)
          "TOP #{visit o.expr}"
        end

        def visit_Arel_Nodes_SelectStatement(o)
          [
            "SELECT",
            (visit(o.limit) if o.limit),
            (visit(o.offset) if o.offset),
            o.cores.map { |x| visit_Arel_Nodes_SelectCore x }.join,
            ("ORDER BY #{o.orders.map { |x| visit x }.join(", ")}" unless o.orders.empty?),
          ].compact.join " "
        end

        def visit_Arel_Nodes_SelectCore(o)
          [
            "#{o.projections.map { |x| visit x }.join ", "}",
            ("FROM #{visit o.source}" if o.source),
            ("WHERE #{o.wheres.map { |x| visit x }.join " AND "}" unless o.wheres.empty?),
            ("GROUP BY #{o.groups.map { |x| visit x }.join ", "}" unless o.groups.empty?),
            (visit(o.having) if o.having),
          ].compact.join " "
        end
      end
    end
  end
end
