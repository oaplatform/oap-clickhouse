/*
 * The MIT License (MIT)
 *
 * Copyright (c) Open Application Platform Authors
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

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

import static java.util.stream.Collectors.toList;
import static oap.clickhouse.migration.ViewInfo.AggregatorFunction.Function.count;

@ToString
public class ViewInfo {
    public final String name;
    public final boolean materialized;
    public final boolean populate;
    public final List<AggregatorFunction> aggregates;
    public final String fromTable;
    public final List<Field> fields;
    public final Optional<String> where;
    public final ArrayList<String> pk = new ArrayList<>();
    public final Optional<String> toTable;

    public ViewInfo( String name, boolean materialized, boolean populate,
                     List<Field> fields,
                     List<AggregatorFunction> aggregates,
                     Optional<String> where,
                     String fromTable,
                     Optional<String> toTable ) {
        this.name = name;
        this.materialized = materialized;
        this.populate = populate;
        this.fields = fields;
        this.aggregates = aggregates;
        this.where = where;
        this.fromTable = fromTable;
        this.toTable = toTable;

        Preconditions.checkArgument( !populate || toTable.isEmpty() );
    }

    public boolean equalFields( Collection<String> fields, Collection<AggregatorFunction> aggregates ) {
        return new HashSet<>( fields ).equals( new HashSet<>( Lists.map( this.fields, f -> f.name ) ) )
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
    public static class Field {
        public final String name;
        public final String alias;

        public Field( String name, String alias ) {
            this.name = name;
            this.alias = alias;
        }

        public static Field of( String name, String alias ) {
            return new Field( name, alias );
        }

        public static Field of( String name ) {
            return of( name, null );
        }

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
