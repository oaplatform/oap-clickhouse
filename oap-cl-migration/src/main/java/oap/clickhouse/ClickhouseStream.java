package oap.clickhouse;


import lombok.extern.slf4j.Slf4j;
import oap.io.Closeables;
import oap.util.Throwables;
import org.apache.commons.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;

import static java.net.HttpURLConnection.HTTP_OK;

/**
 * Created by igor.petrenko on 2019-10-15.
 */
@Slf4j
public class ClickhouseStream implements Closeable {
    private final OutputStream outputStream;
    private final HttpURLConnection http;
    private InputStream inputStream = null;

    public ClickhouseStream( OutputStream os, HttpURLConnection http ) {
        this.outputStream = os;
        this.http = http;
    }

    @Override
    public void close() {
        try( var ignored = getInputStream() ) {

        } catch( IOException e ) {
            throw Throwables.propagate( e );
        }
    }

    public OutputStream getOutputStream() {
        return new CLOutputStream( outputStream );
    }

    public InputStream getInputStream() {
        try {
            if( inputStream == null ) {
                outputStream.flush();
                Closeables.close( outputStream );

                if( http.getResponseCode() != HTTP_OK ) {
                    var body = IOUtils.toString( http.getErrorStream(), StandardCharsets.UTF_8 );
                    log.error( "url code = {}, body = {}", http.getResponseCode(), body );
                    throw new ClickHouseException( "", http.getResponseCode(), body );
                }

                inputStream = http.getInputStream();
            }

            return inputStream;
        } catch( IOException e ) {
            throw Throwables.propagate( e );
        }
    }

    private class CLOutputStream extends java.io.OutputStream {
        private final OutputStream outputStream;

        private CLOutputStream( OutputStream outputStream ) {
            this.outputStream = outputStream;
        }

        @Override
        public void write( int i ) throws IOException {
            outputStream.write( i );
        }

        @Override
        public void write( byte[] b ) throws IOException {
            outputStream.write( b );
        }

        @Override
        public void write( byte[] b, int off, int len ) throws IOException {
            outputStream.write( b, off, len );
        }

        @Override
        public void flush() throws IOException {
            outputStream.flush();
        }

        @Override
        public void close() {
            ClickhouseStream.this.close();
        }
    }
}
