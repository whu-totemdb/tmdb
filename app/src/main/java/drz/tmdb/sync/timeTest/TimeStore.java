package drz.tmdb.sync.timeTest;

public class TimeStore {
    public long requestStartTime;

    public long requestSendTime;//请求发送时刻

    public long requestProcessTime;

    public long responseReceiveTime;//响应接收时刻

    public long endTime;

    public String show(){
        StringBuilder sb = new StringBuilder();
        sb.append("整个过程耗时为："+(endTime-requestStartTime)+"ms");
        sb.append("\n");

        sb.append("请求处理耗时为："+(requestSendTime-requestStartTime)+"ms");
        sb.append("\n");

        sb.append("请求在另一个节点处理耗时为："+requestProcessTime+"ms");
        sb.append("\n");

        sb.append("响应在本地处理完成并仲裁成功耗时为:"+(endTime - responseReceiveTime)+"ms");
        sb.append("\n");

        sb.append("传输时延为："+(responseReceiveTime - requestSendTime - requestProcessTime)+"ms");

        System.out.println("整个过程耗时为："+(endTime-requestStartTime)+"ms");
        System.out.println("请求处理耗时为："+(requestSendTime-requestStartTime)+"ms");
        System.out.println("请求在另一个节点处理耗时为："+requestProcessTime+"ms");
        System.out.println("响应在本地处理完成并仲裁成功耗时为:"+(endTime - responseReceiveTime)+"ms");

        return sb.toString();


    }
}
