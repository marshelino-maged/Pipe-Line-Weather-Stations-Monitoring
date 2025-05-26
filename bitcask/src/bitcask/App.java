package bitcask;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class App {
    public static void main(String[] args) throws Exception {
        BitCaskEngine engine = new BitCaskEngine("D:\\Study\\Semester 8\\Data Intensive\\Project\\Pipe-Line-Weather-Stations-Monitoring\\data\\bitcask", 1024 * 1024, true);

        if (args.length == 0 || args[0].equals("--help")) {
            System.out.println("Usage:");
            System.out.println("  --view-all");
            System.out.println("  --view --key=SOME_KEY");
            System.out.println("  --perf --clients=NUM");
            return;
        }

        if (args[0].equals("--view-all")) {
            String filename = System.currentTimeMillis() + ".csv";
            try (PrintWriter writer = new PrintWriter(new FileWriter(filename))) {
                writer.println("key,value");
                for (Map.Entry<String, String> entry : engine.dumpAll().entrySet()) {
                    writer.printf("%s,%s%n", entry.getKey(), entry.getValue());
                }
            }
            System.out.println("Written to: " + filename);
        } else if (args[0].equals("--view") && args[1].startsWith("--key=")) {
            String key = args[1].substring(6);
            String value = engine.get(key);
            System.out.println(value != null ? value : "(key not found)");
        } else if (args[0].equals("--perf") && args[1].startsWith("--clients=")) {
            int clients = Integer.parseInt(args[1].substring(10));
            for (int i = 0; i < clients; i++) {
                int threadNum = i + 1;
                new Thread(() -> {
                    try {
                        String filename = System.currentTimeMillis() + "_thread_" + threadNum + ".csv";
                        try (PrintWriter writer = new PrintWriter(new FileWriter(filename))) {
                            writer.println("key,value");
                            for (Map.Entry<String, String> entry : engine.dumpAll().entrySet()) {
                                writer.printf("%s,%s%n", entry.getKey(), entry.getValue());
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }).start();
            }
        } else {
            System.out.println("Invalid arguments. Use --help for usage.");
        }
    }
}
