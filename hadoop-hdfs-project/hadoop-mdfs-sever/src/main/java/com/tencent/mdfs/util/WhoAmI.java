package com.tencent.mdfs.util;

/**
 * create:chunxiaoli
 * Date:4/25/18
 */
public class WhoAmI {
    /**
     * The name of the main class of this program == program name.
     */
    private static String programName = null;

    /**
     * Returns the program name (=main class name).
     * <p>
     * Determines the name of the first class loaded for this program by means of a probing Exception.<BR>
     * The name is saved, so the evaluation is performed only once.
     * </P>
     *
     * @return the name of the program.
     */
    public static String getProgramName() {
        try {
            if (programName == null) {
                try {
                    throw new Exception();
                } catch (Exception e) {
                    StackTraceElement[] u = e.getStackTrace();

                    programName = u[u.length - 1].getClassName();
                    if (programName != null) {
                        String arr[]= programName.split("\\.");
                        programName=arr[arr.length-1];
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("WhoAmI getProgramName err:"+e);
        }
        return (programName);
    }
}
