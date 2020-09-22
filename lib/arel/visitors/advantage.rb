module Arel
  module Visitors
    class Advantage < Arel::Visitors::ToSql
      private

      def visit_Arel_Nodes_Offset(o, collector)
        collector << "START AT "
        visit (o.expr + 1), collector
        collector << " "
      end

      def visit_Arel_Nodes_Limit(o, collector)
        collector << "TOP  "
        visit o.expr, collector
        collector << " "
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

        if o.havings.any?
          collector << " HAVING "
          collector = inject_join o.havings, collector, " AND "
        end
        collector
      end
    end
  end
end

# Arel::Visitors::VISITORS['advantage'] = Arel::Visitors::Advantage
