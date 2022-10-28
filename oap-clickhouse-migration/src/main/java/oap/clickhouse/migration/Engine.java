/*
 * Copyright (c) Xenoss
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 */

package oap.clickhouse.migration;

import lombok.EqualsAndHashCode;

import java.util.ArrayList;

@EqualsAndHashCode
public class Engine {
    public final EngineType type;
    public final ArrayList<String> parameters = new ArrayList<>();

    public Engine( EngineType type ) {
        this.type = type;
    }

    public static Engine fromString( String engine ) {
        var params = engine.indexOf( '(' );
        if( params > 0 ) {
            var newEngine = new Engine( EngineType.valueOf( engine.substring( 0, params ) ) );

            var endParams = engine.indexOf( ')', params );

            newEngine.parameters.add( engine.substring( params + 1, endParams ) );

            return newEngine;
        } else {
            return new Engine( EngineType.valueOf( engine ) );
        }
    }

    @Override
    public String toString() {
        return type + ( parameters.isEmpty() ? "" : "(" + parameters.get( 0 ) + ")" );
    }
}
