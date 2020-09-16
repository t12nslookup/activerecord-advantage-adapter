module Arel
  module Visitors
    class Advantage < Arel::Visitors::ToSql
      private

      def visit_Arel_Nodes_Offset o
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
          ("ORDER BY #{o.orders.map { |x| visit x }.join(', ')}" unless o.orders.empty?),
          ].compact.join ' '
      end

      def visit_Arel_Nodes_SelectCore o
        [
          "#{o.projections.map { |x| visit x }.join ', '}",
          #("FROM #{visit o.froms}" if o.froms),
          ("FROM #{visit o.source}" if o.source),
          ("WHERE #{o.wheres.map { |x| visit x }.join ' AND ' }" unless o.wheres.empty?),
          ("GROUP BY #{o.groups.map { |x| visit x }.join ', ' }" unless o.groups.empty?),
          (visit(o.having) if o.having),
        ].compact.join ' '
      end
    end
  end
end

# Arel::Visitors::VISITORS['advantage'] = Arel::Visitors::Advantage

