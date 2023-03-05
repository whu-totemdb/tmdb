package drz.tmdb.sync.util;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

public class NetworkUtil {

    //获取本机IPV4地址
    public static String getLocalHostIP() throws Exception
    {
        String ipaddress = "";
        try
        {
            Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();

            // 遍历所用的网络接口
            while (en.hasMoreElements())
            {
                NetworkInterface nif = en.nextElement();// 得到每一个网络接口绑定的所有ip
                Enumeration<InetAddress> inet = nif.getInetAddresses();
                // 遍历每一个接口绑定的所有ip
                while (inet.hasMoreElements())
                {
                    InetAddress ip = inet.nextElement();
                    if (ip != null && ip instanceof  Inet4Address && !ip.isLoopbackAddress() && isIPv4Address(ip.getHostAddress()))
                    {
                        ipaddress = ip.getHostAddress();
                        return ipaddress;
                    }
                }

            }
        }
        catch (SocketException e)
        {
            System.out.println("获取本地ip地址失败");
            e.printStackTrace();
        }
        return ipaddress;

    }

    public static boolean isIPv4Address(String ip) throws Exception{

        String arrays[]=ip.split("\\.");//.转义
        if (arrays.length!=4) {

            return false;
        }
        for(int i=0;i<arrays.length;i++) {
            Integer num=null;
            try {
                num=Integer.parseInt(arrays[i]);
                if (num<0||num>255) {
                    return false;
                }
                if (!num.equals("0")&&arrays[i].startsWith("0")) {//避免01出现
                    return false;
                }

            } catch (NumberFormatException e) {

                return false;
            }
        }
        return true;

    }

    //int转IPV4地址
    public static String int2IP(int ipInt){
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(ipInt & 0xFF).append(".");
        stringBuilder.append((ipInt >> 8) & 0xFF).append(".");
        stringBuilder.append((ipInt >> 16) & 0xFF).append(".");
        stringBuilder.append((ipInt >> 24) & 0xFF);

        return stringBuilder.toString();
    }

    // 获取本机网络的广播地址
    public static ArrayList<InetAddress> getBroadcastAddresses() throws SocketException {
        ArrayList<InetAddress> list = new ArrayList<>();
        Enumeration<NetworkInterface> enumeration = NetworkInterface.getNetworkInterfaces();

        while (enumeration.hasMoreElements()) {
            NetworkInterface networkInterface = enumeration.nextElement(); // 获取单个网络接口（或者说网卡）
            if (!networkInterface.isUp() || networkInterface.isLoopback() || networkInterface.isVirtual()) {
                // 过滤掉未启用接口、回环接口、虚拟接口 （这些依据自己情况调整）
                continue;
            }

            List<InterfaceAddress> interfaceAddresses = networkInterface.getInterfaceAddresses(); // 接口下地址列表
            for (InterfaceAddress interfaceAddress : interfaceAddresses) {
                InetAddress inetAddress = interfaceAddress.getAddress(); // 获取IP地址
                if ((inetAddress == null) || !(inetAddress instanceof Inet4Address)) {
                    // 如果地址为空或者非IPv4地址则跳过
                    continue;
                }
                list.add(interfaceAddress.getBroadcast()); // 将广播地址添加到列表中
            }
        }
        return list;
    }
}
