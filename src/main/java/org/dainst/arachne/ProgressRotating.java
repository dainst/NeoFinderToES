package org.dainst.arachne;

/**
 *
 */
class ProgressRotating extends Thread {

        private boolean terminate = false;
        private boolean paused = false;
        
        @Override
        public void run() {
            String anim = "|/-\\";
            int x = 0;
            while (!terminate) {
                if (!paused) {
                    System.out.print("\r" + anim.charAt(x++ % anim.length()));
                } else {
                    System.out.print("\r");
                }
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
        
        public void pause() {
            paused = true;
        }
        
        public void unpause() {
            paused = false;
        }
}
