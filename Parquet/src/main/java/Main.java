import org.apache.avro.Schema;


public class Main {


    public static void main(String[] args) throws Exception {

        WeatherDataArchiver archiver = new WeatherArchiverImpl();
        String filename = "C:\\Users\\LENOVO\\Desktop\\Parquet\\src\\main\\resources\\weather_status.avsc";
        Schema schema = archiver.loadSchema(filename);
        String jsonData = "[\n" +
                "    {\n" +
                "      \"station_id\": 1,\n" +
                "      \"s_no\": 1,\n" +
                "      \"battery_status\": \"high\",\n" +
                "      \"status_timestamp\": 1748092835,\n" +
                "      \"weather\": {\n" +
                "        \"humidity\": 55,\n" +
                "        \"temperature\": 31,\n" +
                "        \"wind_speed\": 10\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"station_id\": 1,\n" +
                "      \"s_no\": 2,\n" +
                "      \"battery_status\": \"medium\",\n" +
                "      \"status_timestamp\": 1748092835,\n" +
                "      \"weather\": {\n" +
                "        \"humidity\": 64,\n" +
                "        \"temperature\": 40,\n" +
                "        \"wind_speed\": 18\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"station_id\": 1,\n" +
                "      \"s_no\": 3,\n" +
                "      \"battery_status\": \"high\",\n" +
                "      \"status_timestamp\": 1748092835,\n" +
                "      \"weather\": {\n" +
                "        \"humidity\": 57,\n" +
                "        \"temperature\": 24,\n" +
                "        \"wind_speed\": 6\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"station_id\": 2,\n" +
                "      \"s_no\": 1,\n" +
                "      \"battery_status\": \"medium\",\n" +
                "      \"status_timestamp\": 1748092835,\n" +
                "      \"weather\": {\n" +
                "        \"humidity\": 35,\n" +
                "        \"temperature\": 28,\n" +
                "        \"wind_speed\": 23\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"station_id\": 2,\n" +
                "      \"s_no\": 2,\n" +
                "      \"battery_status\": \"medium\",\n" +
                "      \"status_timestamp\": 1748092835,\n" +
                "      \"weather\": {\n" +
                "        \"humidity\": 55,\n" +
                "        \"temperature\": 36,\n" +
                "        \"wind_speed\": 6\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"station_id\": 2,\n" +
                "      \"s_no\": 3,\n" +
                "      \"battery_status\": \"high\",\n" +
                "      \"status_timestamp\": 1748092836,\n" +
                "      \"weather\": {\n" +
                "        \"humidity\": 61,\n" +
                "        \"temperature\": 30,\n" +
                "        \"wind_speed\": 13\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"station_id\": 3,\n" +
                "      \"s_no\": 1,\n" +
                "      \"battery_status\": \"medium\",\n" +
                "      \"status_timestamp\": 1748092836,\n" +
                "      \"weather\": {\n" +
                "        \"humidity\": 65,\n" +
                "        \"temperature\": 45,\n" +
                "        \"wind_speed\": 13\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"station_id\": 3,\n" +
                "      \"s_no\": 2,\n" +
                "      \"battery_status\": \"high\",\n" +
                "      \"status_timestamp\": 1748092836,\n" +
                "      \"weather\": {\n" +
                "        \"humidity\": 80,\n" +
                "        \"temperature\": 30,\n" +
                "        \"wind_speed\": 21\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"station_id\": 3,\n" +
                "      \"s_no\": 3,\n" +
                "      \"battery_status\": \"high\",\n" +
                "      \"status_timestamp\": 1748092836,\n" +
                "      \"weather\": {\n" +
                "        \"humidity\": 41,\n" +
                "        \"temperature\": 35,\n" +
                "        \"wind_speed\": 23\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"station_id\": 4,\n" +
                "      \"s_no\": 1,\n" +
                "      \"battery_status\": \"high\",\n" +
                "      \"status_timestamp\": 1748092836,\n" +
                "      \"weather\": {\n" +
                "        \"humidity\": 75,\n" +
                "        \"temperature\": 45,\n" +
                "        \"wind_speed\": 13\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"station_id\": 4,\n" +
                "      \"s_no\": 2,\n" +
                "      \"battery_status\": \"medium\",\n" +
                "      \"status_timestamp\": 1748092836,\n" +
                "      \"weather\": {\n" +
                "        \"humidity\": 58,\n" +
                "        \"temperature\": 28,\n" +
                "        \"wind_speed\": 22\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"station_id\": 4,\n" +
                "      \"s_no\": 3,\n" +
                "      \"battery_status\": \"high\",\n" +
                "      \"status_timestamp\": 1748092836,\n" +
                "      \"weather\": {\n" +
                "        \"humidity\": 50,\n" +
                "        \"temperature\": 39,\n" +
                "        \"wind_speed\": 24\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"station_id\": 5,\n" +
                "      \"s_no\": 1,\n" +
                "      \"battery_status\": \"medium\",\n" +
                "      \"status_timestamp\": 1748092836,\n" +
                "      \"weather\": {\n" +
                "        \"humidity\": 45,\n" +
                "        \"temperature\": 38,\n" +
                "        \"wind_speed\": 7\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"station_id\": 5,\n" +
                "      \"s_no\": 2,\n" +
                "      \"battery_status\": \"medium\",\n" +
                "      \"status_timestamp\": 1748092836,\n" +
                "      \"weather\": {\n" +
                "        \"humidity\": 56,\n" +
                "        \"temperature\": 31,\n" +
                "        \"wind_speed\": 25\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"station_id\": 5,\n" +
                "      \"s_no\": 3,\n" +
                "      \"battery_status\": \"medium\",\n" +
                "      \"status_timestamp\": 1748092836,\n" +
                "      \"weather\": {\n" +
                "        \"humidity\": 34,\n" +
                "        \"temperature\": 35,\n" +
                "        \"wind_speed\": 21\n" +
                "      }\n" +
                "    }\n" +
                "  ]";
        archiver.WriteParquet(jsonData,"archive",schema);
    }
}
