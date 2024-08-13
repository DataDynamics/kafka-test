package io.datadynamics.pilot.kafka;

public class PlatformCallbackImpl extends PlatformCallback {

    @Override
    void onCompletion(String messageId, Exception exception) {
        System.out.println("수신 실패 : " + messageId);
    }

}
