package com.tencent.mdfs.util;

import java.io.PrintStream;
import java.util.Locale;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


/**
 * create:lichunxiao
 * Date:2019-04-23
 */
public class StdOutErrLog {


    private static final Logger LOGGER = LogManager.getLogger("std");

    public static void setSystemOutAndErrToLog() {
        System.setOut(createLoggingProxy(System.out));
        System.setErr(createLoggingProxy(System.err));
    }

    public static PrintStream createLoggingProxy(final PrintStream realPrintStream) {
        return new PrintStream(realPrintStream) {
            @Override
            public void print(final String string) {
                realPrintStream.print(string);
                LOGGER.info(string);
            }

            @Override
            public void write(int b) {
                super.write(b);
            }

            @Override
            public void write(byte[] buf, int off, int len) {
                super.write(buf, off, len);
            }

            @Override
            public void print(boolean b) {
                LOGGER.info(b);
            }

            @Override
            public void print(char c) {
                LOGGER.info(c);
            }

            @Override
            public void print(int i) {
                LOGGER.info(i);
            }

            @Override
            public void print(long l) {
                LOGGER.info(l);
            }

            @Override
            public void print(float f) {
                LOGGER.info(f);
            }

            @Override
            public void print(double d) {
                LOGGER.info(d);
            }

            @Override
            public void print(char[] s) {
                LOGGER.info(s);
            }

            @Override
            public void print(Object obj) {
                LOGGER.info(obj);
            }

            @Override
            public void println() {
                super.println();
            }

            @Override
            public void println(boolean x) {
                LOGGER.info(x);
            }

            @Override
            public void println(char x) {
                LOGGER.info(x);
            }

            @Override
            public void println(int x) {
                LOGGER.info(x);
            }

            @Override
            public void println(long x) {
                LOGGER.info(x);
            }

            @Override
            public void println(float x) {
                LOGGER.info(x);
            }

            @Override
            public void println(double x) {
                LOGGER.info(x);
            }

            @Override
            public void println(char[] x) {
                LOGGER.info(x);
            }

            @Override
            public void println(String x) {
                LOGGER.info(x);
            }

            @Override
            public void println(Object x) {
                LOGGER.info(x);
            }

            @Override
            public PrintStream printf(String format, Object... args) {
                return super.printf(format, args);
            }

            @Override
            public PrintStream printf(Locale l, String format, Object... args) {
                return super.printf(l, format, args);
            }
        };
    }
}
