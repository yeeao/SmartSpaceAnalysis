package edu.uci.ics.localization;

import edu.uci.ics.metadata.Metadata;
import edu.uci.ics.metadata.config;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

// This code is for parallel processing with locater.

public class directQuery {

    private String PREDICTION_RESULT2 = "prediction_result2";
    private String PREDICTION_RESULT3 = "prediction_result3";
    private String PREDICTION_RESULT4 = "prediction_result4";
    private String WAITLIST = "waitlist";
    private String WAITLIST3 = "waitlist3";
    private String WAITLIST4 = "waitlist4";

    public class EventsList {
        public ArrayList<Integer> ids;
        public ArrayList<String> macs;
        public ArrayList<String> sensors;
        public ArrayList<String> timestamp;

        public EventsList() {
            ids = new ArrayList<>();
            macs = new ArrayList<>();
            sensors = new ArrayList<>();
            timestamp = new ArrayList<>();
        }

        public void addEvents(int id, String mac, String sensor, String time) {
            ids.add(id);
            macs.add(mac);
            sensors.add(sensor);
            timestamp.add(time);
        }

    }
    public static void main(String[] args) {
        config.loadConfiguration();
        Metadata.load();

        long start = System.currentTimeMillis();
        directQuery dq = new directQuery();
        try {
            dq.main_sub();
        } catch (InterruptedException e) {

        }
//        for (int i = 0; i < 500; i++) {
//            System.out.println("Current round: " + i);
//            dq.processBundle();
//        }
//        long finish = System.currentTimeMillis();
//        double timeElapsed = (double) (finish - start) / 1000.0;
//        System.out.printf("Total running time (seconds): %s%n",timeElapsed);
    }

    public void processBundle() {
        int data_count = 0;
        ArrayList<Integer> waillist_ids = new ArrayList<>();

        try (Connect connect = new Connect("local", "postgres")) {
            long start = System.currentTimeMillis();

            PreparedStatement p = connect.connection.prepareStatement(
                    String.format("SELECT id, payload, \"timestamp\", sensor_id FROM public.waitlist limit 100;"));
            ResultSet res = p.executeQuery();
            PreparedStatement p2 = connect.connection.prepareStatement(String.format("insert into %s values(?,?,?,?)",
                    PREDICTION_RESULT2));

            coarseLocalization cl = new coarseLocalization();

            while (res.next()) {
                data_count++;
                waillist_ids.add(res.getInt("id"));
                String mac = res.getString("payload");
                String timestamp = res.getString("timestamp");
                int lengthOfData = 14;

                int loc = cl.coarseLocalization(mac, timestamp, lengthOfData);

//                System.out.println(res.getString("sensor_id"));
                System.out.println(loc);

                p2.setInt(1, res.getInt("id"));
                p2.setString(2, mac);
                p2.setString(3, res.getString("sensor_id"));
                p2.setString(4, String.valueOf(loc));
                p2.addBatch();
            }

            // Save result to db
            p2.executeBatch();

            // delete data from waitlist
            String ids = listToString(waillist_ids);
            PreparedStatement p3 = connect.connection.prepareStatement(
                    String.format("DELETE FROM waitlist where id in (%s)", ids));
            p3.execute();

            long finish = System.currentTimeMillis();
            double timeElapsed = (double) (finish - start) / 1000.0;
            System.out.printf("Total processing time (seconds): %s%n",timeElapsed);
            System.out.println("Actual number of data processed: " + data_count);
            double ptpd = (double) timeElapsed / data_count;
            System.out.println("Processing Time per data: " + ptpd);

        } catch (Exception e) {

        }
    }

    public String listToString(ArrayList<Integer> a) {

        StringBuilder res = new StringBuilder();
        for (int i = 0; i < a.size(); i++) {
            if (i == a.size()-1) {
                res.append(a.get(i));
            } else {
                res.append(a.get(i)).append(",");
            }
        }

        return res.toString();
    }

    public void main_sub() throws InterruptedException {
        long start = System.currentTimeMillis();
        directQuery dq = new directQuery();
        for (int i = 0; i < 3000; i++) {
            System.out.println("Current round: " + i);

            EventsList e1 = new EventsList();
            EventsList e2 = new EventsList();
            EventsList e3 = new EventsList();

            // get data
            try (Connect connect = new Connect("local", "postgres")) {
                PreparedStatement p = connect.connection.prepareStatement(
                        String.format("SELECT id, payload, \"timestamp\", sensor_id FROM public.waitlist order by \"timestamp\" limit 3000;"));
                ResultSet res = p.executeQuery();

                int splitter = 1;
                while(res.next()) {
                    if (splitter == 1) {
                        e1.addEvents(res.getInt("id"), res.getString("payload"),
                                res.getString("sensor_id"), res.getString("timestamp"));
                        splitter = 2;
                    } else if (splitter == 2) {
                        e2.addEvents(res.getInt("id"), res.getString("payload"),
                                res.getString("sensor_id"), res.getString("timestamp"));
                        splitter = 3;
                    } else if (splitter == 3) {
                        e3.addEvents(res.getInt("id"), res.getString("payload"),
                                res.getString("sensor_id"), res.getString("timestamp"));
                        splitter = 1;
                    }
                }
//                System.out.println(e1.ids.size());
//                System.out.println(e2.ids.size());
//                System.out.println(e3.ids.size());
            } catch (Exception e) {

            }

            Callable<Void> call_1 = () -> {
//                dq.processBundle();
                dq.processBundle(e1);
                return null;
            };


            Callable<Void> call_2 = () -> {
//                dq.processBundle3();
                dq.processBundle(e2);
                return null;
            };

            Callable<Void> call_3 = () -> {
//                dq.processBundle4();
                dq.processBundle(e3);
                return null;
            };



            List<Callable<Void>> tasks = new ArrayList<>();
            tasks.add(call_1);
            tasks.add(call_2);
            tasks.add(call_3);

            ExecutorService executorService = Executors.newFixedThreadPool(3);

            try {
                executorService.invokeAll(tasks);
            } catch (InterruptedException e) {

            }

            executorService.shutdownNow();
            if (!executorService.awaitTermination(100, TimeUnit.MICROSECONDS)) {
                System.out.println("Still waiting...");
//                long finish = System.currentTimeMillis();
//                double timeElapsed = (double) (finish - start) / 1000.0;
//                System.out.printf("Total running time (seconds): %s%n",timeElapsed);
//                System.exit(0);
                System.out.println("Starting again");
                main_sub();
                System.exit(0);
            }
            System.out.println("Exiting normally...");
            long finish = System.currentTimeMillis();
            double timeElapsed = (double) (finish - start) / 1000.0;
            System.out.printf("Total running time (seconds): %s%n",timeElapsed);
        }
    }

    public void processBundle3() {
        int data_count = 0;
        ArrayList<Integer> waillist_ids = new ArrayList<>();

        try (Connect connect = new Connect("local", "postgres")) {
            long start = System.currentTimeMillis();

            PreparedStatement p = connect.connection.prepareStatement(
                    String.format("SELECT id, payload, \"timestamp\", sensor_id FROM public.waitlist3 limit 100;"));
            ResultSet res = p.executeQuery();
            PreparedStatement p2 = connect.connection.prepareStatement(String.format("insert into %s values(?,?,?,?)",
                    PREDICTION_RESULT3));

            coarseLocalization cl = new coarseLocalization();

            while (res.next()) {
                data_count++;
                waillist_ids.add(res.getInt("id"));
                String mac = res.getString("payload");
                String timestamp = res.getString("timestamp");
                int lengthOfData = 14;

                int loc = cl.coarseLocalization(mac, timestamp, lengthOfData);

//                System.out.println(res.getString("sensor_id"));
                System.out.println(loc);

                p2.setInt(1, res.getInt("id"));
                p2.setString(2, mac);
                p2.setString(3, res.getString("sensor_id"));
                p2.setString(4, String.valueOf(loc));
                p2.addBatch();
            }

            // Save result to db
            p2.executeBatch();

            // delete data from waitlist
            String ids = listToString(waillist_ids);
            PreparedStatement p3 = connect.connection.prepareStatement(
                    String.format("DELETE FROM waitlist3 where id in (%s)", ids));
            p3.execute();

            long finish = System.currentTimeMillis();
            double timeElapsed = (double) (finish - start) / 1000.0;
            System.out.printf("Total processing time (seconds): %s%n",timeElapsed);
            System.out.println("Actual number of data processed: " + data_count);
            double ptpd = (double) timeElapsed / data_count;
            System.out.println("Processing Time per data: " + ptpd);

        } catch (Exception e) {

        }
    }

    public void processBundle4() {
        int data_count = 0;
        ArrayList<Integer> waillist_ids = new ArrayList<>();

        try (Connect connect = new Connect("local", "postgres")) {
            long start = System.currentTimeMillis();

            PreparedStatement p = connect.connection.prepareStatement(
                    String.format("SELECT id, payload, \"timestamp\", sensor_id FROM public.waitlist4 limit 100;"));
            ResultSet res = p.executeQuery();
            PreparedStatement p2 = connect.connection.prepareStatement(String.format("insert into %s values(?,?,?,?)",
                    PREDICTION_RESULT4));

            coarseLocalization cl = new coarseLocalization();

            while (res.next()) {
                data_count++;
                waillist_ids.add(res.getInt("id"));
                String mac = res.getString("payload");
                String timestamp = res.getString("timestamp");
                int lengthOfData = 14;

                int loc = cl.coarseLocalization(mac, timestamp, lengthOfData);

//                System.out.println(res.getString("sensor_id"));
                System.out.println(loc);

                p2.setInt(1, res.getInt("id"));
                p2.setString(2, mac);
                p2.setString(3, res.getString("sensor_id"));
                p2.setString(4, String.valueOf(loc));
                p2.addBatch();
            }

            // Save result to db
            p2.executeBatch();

            // delete data from waitlist
            String ids = listToString(waillist_ids);
            PreparedStatement p3 = connect.connection.prepareStatement(
                    String.format("DELETE FROM waitlist4 where id in (%s)", ids));
            p3.execute();

            long finish = System.currentTimeMillis();
            double timeElapsed = (double) (finish - start) / 1000.0;
            System.out.printf("Total processing time (seconds): %s%n",timeElapsed);
            System.out.println("Actual number of data processed: " + data_count);
            double ptpd = (double) timeElapsed / data_count;
            System.out.println("Processing Time per data: " + ptpd);

        } catch (Exception e) {

        }
    }

    public void processBundle(EventsList events) {
        int data_count = 0;

        try (Connect connect = new Connect("local", "postgres")) {
            long start = System.currentTimeMillis();
            PreparedStatement p2 = connect.connection.prepareStatement(String.format("insert into %s values(?,?,?,?)",
                    PREDICTION_RESULT2));

            coarseLocalization cl = new coarseLocalization();

            for (int i = 0; i < events.ids.size(); i++) {
                data_count++;
                int lengthOfData = 14;

                int loc = cl.coarseLocalization(events.macs.get(i), events.timestamp.get(i), lengthOfData);

                System.out.println(loc);

                p2.setInt(1, events.ids.get(i));
                p2.setString(2, events.macs.get(i));
                p2.setString(3, events.sensors.get(i));
                p2.setString(4, String.valueOf(loc));
                p2.addBatch();
            }

            // Save result to db
            p2.executeBatch();

            // delete data from waitlist
            String ids = listToString(events.ids);
            PreparedStatement p3 = connect.connection.prepareStatement(
                    String.format("DELETE FROM waitlist where id in (%s)", ids));
            p3.execute();

            long finish = System.currentTimeMillis();
            double timeElapsed = (double) (finish - start) / 1000.0;
            System.out.printf("Total processing time (seconds): %s%n",timeElapsed);
//            System.out.println("Actual number of data processed: " + data_count);
            double ptpd = (double) timeElapsed / data_count;
            System.out.println("Processing Time per data: " + ptpd);

        } catch (Exception e) {

        }
    }

}
