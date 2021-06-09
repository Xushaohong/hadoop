package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.Schedulable;

class FairCommonComparator {
    public static int trivalCompare(Schedulable s1, Schedulable s2) {
        int res = (int) Math.signum(s1.getStartTime() - s2.getStartTime());
        if (res != 0) {
            return res;
        }
        // now they have same startTime
        return s1.getName().compareTo(s2.getName());
    }

    public static int basicCompare(Schedulable s1, Schedulable s2) {
        // (s1 < s2) means s1 first

        //now both are runnable
        if (s1.isNeedResource() && !s2.isNeedResource()) {
            return -1;
        } else if (!s1.isNeedResource() && s2.isNeedResource()) {
            return 1;
        } else if (!s1.isNeedResource() && !s2.isNeedResource()){
            //both are non needingResource, short circuit the compare
            return trivalCompare(s1, s2);
        }

        //now both are runnable and needingResource
        return 0;
    }
}
