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
                    throw new ClickhouseException( "", http.getResponseCode(), body );
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
