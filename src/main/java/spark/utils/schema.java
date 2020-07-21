package spark.utils;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class schema {

    private static final StructType SCHEMA = FIELD_SCHEMA();
    public static final Encoder<Row> ENC = RowEncoder.apply(SCHEMA);

    private static StructType FIELD_SCHEMA() {
        List<StructField> fields = new ArrayList<>();

        fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("salary", DataTypes.IntegerType, true));
        return new StructType(fields.toArray(new StructField[fields.size()]));
    }
}
