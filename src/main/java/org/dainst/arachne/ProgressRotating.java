package org.dainst.arachne;

/**
 *
 */
class ProgressRotating extends Thread {

        boolean showProgress = true;

        @Override
        public void run() {
            String anim = "|/-\\";
            int x = 0;
            while (showProgress) {
                System.out.print("\r" + anim.charAt(x++ % anim.length()));
                try {
                    Thread.sleep(100);
                } catch (Exception e) {
                };
            }
        }
}
