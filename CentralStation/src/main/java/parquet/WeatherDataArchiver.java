package parquet;

import org.apache.avro.Schema;


public interface WeatherDataArchiver {


    /**
     * Loads an Avro schema from a given file path.
     *
     * @param schemaFilePath The path to the Avro schema file (in .avsc format).
     * @return The parsed Avro Schema object.
     * @throws Exception If the schema file cannot be read or parsed.
     */
    Schema loadSchema(String schemaFilePath) throws Exception;



    /**
     * Converts JSON data to Avro GenericRecords using the provided schema,
     * partitions the records, and writes them to Parquet files in the specified base directory.
     *
     * @param JsonData The JSON string containing an array of records to be written.
     * @param baseDir  The base directory where the Parquet partition directories will be created.
     * @param schema   The Avro schema describing the structure of the JSON records.
     * @throws Exception If there is an error during JSON parsing, record generation, or writing to Parquet.
     */
    void WriteParquet(String JsonData, String baseDir , Schema schema) throws Exception;


}
