package dev.mockmq.amqp;

public final class AmqpMethodRegistry {

    private AmqpMethodRegistry() {
    }

    public static String name(int classId, int methodId) {
        return switch (classId) {
            case 10 -> connection(methodId);
            case 20 -> channel(methodId);
            case 40 -> exchange(methodId);
            case 50 -> queue(methodId);
            case 60 -> basic(methodId);
            default -> "Unknown(" + classId + ":" + methodId + ")";
        };
    }

    private static String connection(int methodId) {
        return switch (methodId) {
            case 10 -> "Connection.Start";
            case 11 -> "Connection.Start-Ok";
            case 30 -> "Connection.Tune";
            case 31 -> "Connection.Tune-Ok";
            case 40 -> "Connection.Open";
            case 41 -> "Connection.Open-Ok";
            case 50 -> "Connection.Close";
            case 51 -> "Connection.Close-Ok";
            default -> "Connection.Unknown(" + methodId + ")";
        };
    }

    private static String channel(int methodId) {
        return switch (methodId) {
            case 10 -> "Channel.Open";
            case 11 -> "Channel.Open-Ok";
            case 40 -> "Channel.Close";
            case 41 -> "Channel.Close-Ok";
            default -> "Channel.Unknown(" + methodId + ")";
        };
    }

    private static String exchange(int methodId) {
        return switch (methodId) {
            case 10 -> "Exchange.Declare";
            case 11 -> "Exchange.Declare-Ok";
            case 20 -> "Exchange.Delete";
            case 21 -> "Exchange.Delete-Ok";
            case 30 -> "Exchange.Bind";
            case 31 -> "Exchange.Bind-Ok";
            case 40 -> "Exchange.Unbind";
            case 51 -> "Exchange.Unbind-Ok";
            default -> "Exchange.Unknown(" + methodId + ")";
        };
    }

    private static String queue(int methodId) {
        return switch (methodId) {
            case 10 -> "Queue.Declare";
            case 11 -> "Queue.Declare-Ok";
            case 20 -> "Queue.Bind";
            case 21 -> "Queue.Bind-Ok";
            case 30 -> "Queue.Purge";
            case 31 -> "Queue.Purge-Ok";
            case 40 -> "Queue.Delete";
            case 41 -> "Queue.Delete-Ok";
            case 50 -> "Queue.Unbind";
            case 51 -> "Queue.Unbind-Ok";
            default -> "Queue.Unknown(" + methodId + ")";
        };
    }

    private static String basic(int methodId) {
        return switch (methodId) {
            case 10 -> "Basic.Qos";
            case 11 -> "Basic.Qos-Ok";
            case 20 -> "Basic.Consume";
            case 21 -> "Basic.Consume-Ok";
            case 30 -> "Basic.Cancel";
            case 31 -> "Basic.Cancel-Ok";
            case 40 -> "Basic.Publish";
            case 50 -> "Basic.Return";
            case 60 -> "Basic.Deliver";
            case 70 -> "Basic.Get";
            case 71 -> "Basic.Get-Ok";
            case 72 -> "Basic.Get-Empty";
            case 80 -> "Basic.Ack";
            case 90 -> "Basic.Reject";
            case 120 -> "Basic.Nack";
            default -> "Basic.Unknown(" + methodId + ")";
        };
    }
}