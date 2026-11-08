import com.sun.tools.attach.VirtualMachine;
import javax.management.*;
import javax.management.openmbean.CompositeData;
import javax.management.remote.*;
import java.util.*;

/** Read-only platform MBeans over the JDK local attach connector. No public JMX port. */
public class JvmSnapshot {
    private static String quote(String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n") + "\"";
    }
    public static void main(String[] args) throws Exception {
        VirtualMachine vm = VirtualMachine.attach(args[0]);
        String address;
        try {
            address = vm.getAgentProperties().getProperty("com.sun.management.jmxremote.localConnectorAddress");
            if (address == null) address = vm.startLocalManagementAgent();
        } finally { vm.detach(); }
        try (JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(address))) {
            MBeanServerConnection c = connector.getMBeanServerConnection();
            CompositeData heap = (CompositeData)c.getAttribute(new ObjectName("java.lang:type=Memory"), "HeapMemoryUsage");
            StringBuilder out = new StringBuilder("{\"heap_used\":").append(heap.get("used"))
                .append(",\"heap_committed\":").append(heap.get("committed"))
                .append(",\"heap_max\":").append(heap.get("max"))
                .append(",\"threads\":").append(c.getAttribute(new ObjectName("java.lang:type=Threading"), "ThreadCount"))
                .append(",\"gc\":[");
            boolean first = true;
            for (ObjectName name : c.queryNames(new ObjectName("java.lang:type=GarbageCollector,*"), null)) {
                if (!first) out.append(',');
                first = false;
                out.append("{\"name\":").append(quote(name.getKeyProperty("name")))
                   .append(",\"collections\":").append(c.getAttribute(name, "CollectionCount"))
                   .append(",\"collection_ms\":").append(c.getAttribute(name, "CollectionTime"))
                   .append('}');
            }
            out.append(']');
            ObjectName mode = new ObjectName("nearhub.benchmark:type=CreateOnlyMode");
            if (c.isRegistered(mode)) {
                out.append(",\"create_only\":").append(c.getAttribute(mode, "Enabled"))
                   .append(",\"skipped_timeout_sends\":").append(c.getAttribute(mode, "SkippedTimeoutSends"))
                   .append(",\"skipped_cancellations\":").append(c.getAttribute(mode, "SkippedCancellations"));
            }
            System.out.println(out.append('}'));
        }
    }
}
