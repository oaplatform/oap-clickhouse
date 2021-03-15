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

package oap.clickhouse;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.StringJoiner;

@Slf4j
@ToString
public class SystemSettings {
    public static final String TABLE_SYSTEM_SETTINGS = "xenoss_system";
    public static final HashMap<String, AbstractParameter<?>> parameters = new HashMap<String, AbstractParameter<?>>();

    private final BooleanParameter preventDestroy = new BooleanParameter( "prevent_destroy", true );
    private final BooleanParameter preventModify = new BooleanParameter( "prevent_modify", true );

    public SystemSettings( ClickhouseClient clickHouseClient ) {
        var database = clickHouseClient.getDatabase();
        var table = database.getTable( TABLE_SYSTEM_SETTINGS );

        if( table.exists() ) {
            var settings = clickHouseClient.getLines( "SELECT * FROM " + TABLE_SYSTEM_SETTINGS );

            for( var line : settings ) {
                var tabIndex = line.indexOf( '\t' );
                var name = line.substring( 0, tabIndex );
                var value = line.substring( tabIndex + 1 );

                log.debug( "name = {}, value = {}", name, value );

                var parameter = parameters.get( name );
                if( parameter == null ) {
                    log.info( "{}={} - unknown settings. Ignored.", name, value );
                    continue;
                }

                parameter.set( value );
            }
        } else {
            clickHouseClient.createDatabase();
            clickHouseClient.execute( "CREATE TABLE " + TABLE_SYSTEM_SETTINGS + " (name String, value String) ENGINE MergeTree ORDER BY name", true );
        }

        clickHouseClient.execute( "TRUNCATE TABLE " + TABLE_SYSTEM_SETTINGS, true );

        var sj = new StringJoiner( ", " );
        for( var parameter : parameters.values() ) {
            sj.add( "('" + parameter.name + "', '" + parameter.getDefaultValueAsString() + "')" );
        }

        clickHouseClient.execute( "INSERT INTO TABLE " + TABLE_SYSTEM_SETTINGS + " FORMAT Values " + sj.toString(), true );

        log.info( "settings = {}", this );
    }

    public boolean isPreventDestroy() {
        return preventDestroy.value;
    }

    public boolean isPreventModify() {
        return preventModify.value;
    }

    public enum Type {
        BOOLEAN, NUMBER, STRING
    }

    @ToString
    public abstract static class AbstractParameter<T> {
        public final String name;
        public T value;
        public T defaultValue;

        public AbstractParameter( String name, T defaultValue ) {
            this.name = name;
            this.value = this.defaultValue = defaultValue;

            SystemSettings.parameters.put( name, this );
        }

        public abstract void set( String value );

        public abstract String getValueAsString();

        public abstract String getDefaultValueAsString();
    }

    @ToString( callSuper = true )
    public static class BooleanParameter extends AbstractParameter<Boolean> {
        public BooleanParameter( String name, Boolean defaultValue ) {
            super( name, defaultValue );
        }

        @Override
        public void set( String value ) {
            this.value = Boolean.parseBoolean( value );
        }

        @Override
        public String getValueAsString() {
            return String.valueOf( value );
        }

        @Override
        public String getDefaultValueAsString() {
            return String.valueOf( defaultValue );
        }
    }
}
