package oap.clickhouse.migration;

import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import oap.util.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import static oap.clickhouse.migration.ViewInfo.AggregatorFunction.Function.count;
import static java.util.stream.Collectors.toList;

/**
 * Created by igor.petrenko on 2019-10-28.
 */
@ToString
public class ViewInfo {
    public final String name;
    public final boolean materialized;
    public final boolean populate;
    public final List<AggregatorFunction> aggregates;
    public final String fromTable;
    public final List<Field> fields;
    public final ArrayList<String> pk = new ArrayList<>();
    public final Optional<String> toTable;

    public ViewInfo( String name, boolean materialized, boolean populate,
                     List<Field> fields,
                     List<AggregatorFunction> aggregates, String fromTable,
                     Optional<String> toTable ) {
        this.name = name;
        this.materialized = materialized;
        this.populate = populate;
        this.fields = fields;
        this.aggregates = aggregates;
        this.fromTable = fromTable;
        this.toTable = toTable;

        Preconditions.checkArgument( !populate || toTable.isEmpty() );
    }

    public boolean equalFields( Collection<String> fields, Collection<AggregatorFunction> aggregates ) {
        return new HashSet<>( fields ).equals( new HashSet<>( this.fields ) )
            && new HashSet<>( aggregates ).equals( new HashSet<>( this.aggregates ) );
    }

    public List<String> getAllFields() {
        return Lists.concat( Lists.map( fields, Field::getFieldName ),
            aggregates.stream().filter( a -> a.function != count ).map( AggregatorFunction::getField ).collect( toList() ) );
    }

    public ViewInfo addPk( String pk ) {
        this.pk.add( pk );
        return this;
    }

    @ToString
    @EqualsAndHashCode
    @AllArgsConstructor
    public static class Field {
        public final String name;
        public final String alias;

        public String getFieldWithAlias() {
            return name + ( alias != null ? " AS " + alias : "" );
        }

        public String getFieldName() {
            return alias != null ? alias : name;
        }
    }

    @AllArgsConstructor
    @EqualsAndHashCode
    public static class AggregatorFunction {
        public final String field;
        public final String alias;
        public final Function function;

        public String getField() {
            return StringUtils.isEmpty( alias ) ? field : alias;
        }

        @Override
        public String toString() {
            var funcField = function != count ? field : "";
            var funcAlias = StringUtils.isEmpty( alias ) ? field : alias;

            return function + "(" + funcField + ") AS " + funcAlias;
        }

        public enum Function {
            sum, count, groupArray, groupUniqArray
        }
    }
}
