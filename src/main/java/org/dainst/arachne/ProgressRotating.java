package org.dainst.arachne;

/**
 *
 */
class ProgressRotating extends Thread {

        private boolean terminate = false;

        @Override
        public void run() {
            String anim = "|/-\\";
            int x = 0;
            while (!terminate) {
                System.out.print("\r" + anim.charAt(x++ % anim.length()));
                try {
                    Thread.sleep(100);
                } catch (Exception e) {
                };
            }
            System.out.print("\r");
        }
        
        public void terminate() {
            terminate = true;
        }
}
