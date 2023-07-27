package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;


public class DataETLPipeline {

    public static void main(String[] args) {

        String jdbcUrl = "jdbc:mysql://localhost:3306/TbDestino"; ///db?allowPublicKeyRetrieval=true&useSSL=false
        String username = "root";
        String password = "root";

        Pipeline pipeline = Pipeline.create();

        // Etapa 1: Deleta os registros do banco de dados de destino antes de prosseguir
        pipeline.apply("DeleteRecordsFromDestination", new DeleteRecordsFromDestinationTransform(jdbcUrl, username, password));

        // Etapa 2: Leia os dados da fonte usando JdbcIO.read()
        pipeline.apply("ReadFromSource", JdbcIO.<Row>read()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("com.mysql.cj.jdbc.Driver",
                                        "jdbc:mysql://localhost:3306/TbOrigem")
                                .withUsername(username)
                                .withPassword(password))
                        .withQuery("SELECT * FROM dados")
                        .withCoder(RowCoder.of(getSchema()))
                        .withRowMapper(new ResultSetToRow()))

                // Etapa 3: Transformacap
                //.apply("TransformData", ParDo.of(new DataTransformationFn()))


                .apply("WriteToDestination", JdbcIO.<Row>write()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("com.mysql.cj.jdbc.Driver",
                                        "jdbc:mysql://localhost:3306/TbDestino")
                                .withUsername(username)
                                .withPassword(password))
                        .withStatement("INSERT INTO dados\n" +
                                "(id, text, alphanumeric, name, email, region, phone)\n" +
                                "VALUES(?, ?, ?, ?, ?, ?, ?);\n")
                        .withPreparedStatementSetter((JdbcIO.PreparedStatementSetter<Row>) (element, preparedStatement) -> {
                            for (int i = 0; i < element.getSchema().getFieldCount(); i++) {
                                Object columnValue = element.getValue(i);
                                preparedStatement.setObject(i + 1, columnValue);
                            }
                        }));

        pipeline.run();
    }

    public static class ResultSetToRow implements JdbcIO.RowMapper<Row> {
        @Override
        public Row mapRow(ResultSet resultSet) throws Exception {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int numColumns = metaData.getColumnCount();

            Schema.Builder schemaBuilder = Schema.builder();
            for (int i = 1; i <= numColumns; i++) {
                String columnName = metaData.getColumnName(i);
                int columnType = metaData.getColumnType(i);
                schemaBuilder.addField(columnName, toFieldType(columnType));

            }

            Schema schema = schemaBuilder.build();
            return buildRow(resultSet, schema);
        }

        private Row buildRow(ResultSet resultSet, Schema schema) throws SQLException {


            Row.Builder rowBuilder = Row.withSchema(schema);

            for (int i = 1; i <= schema.getFieldCount(); i++) {
                Object value = resultSet.getObject(i);
                rowBuilder.addValue(value);
            }

            return rowBuilder.build();
        }

        public static Schema.FieldType toFieldType(int columnType) {
            switch (columnType) {
                case Types.INTEGER:
                case Types.TINYINT:
                case Types.SMALLINT:
                    return Schema.FieldType.INT32;
                case Types.BIGINT:
                    return Schema.FieldType.INT64;
                case Types.FLOAT:
                    return Schema.FieldType.FLOAT;
                case Types.DOUBLE:
                    return Schema.FieldType.DOUBLE;
                case Types.DECIMAL:
                    return Schema.FieldType.DECIMAL;
                case Types.DATE:
                case Types.TIME:
                case Types.TIMESTAMP:
                    return Schema.FieldType.DATETIME;
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.CHAR:
                    return Schema.FieldType.STRING;
                case Types.BLOB:
                case Types.VARBINARY:
                    return Schema.FieldType.BYTES;
                default:
                    throw new IllegalArgumentException("Tipo de coluna desconhecido: " + columnType);
            }
        }

    }

    // Define o esquema do Row
    private static Schema getSchema() {
        return Schema.builder()
                .addInt32Field("id")
                .addStringField("text")
                .addStringField("alphanumeric")
                .addStringField("name")
                .addStringField("email")
                .addStringField("region")
                .addStringField("phone")
                .build();
    }

    // Define um Composite PTransform para deletar registros do banco de dados de destino
    public static class DeleteRecordsFromDestinationTransform extends PTransform<PBegin, PCollection<Void>> {

        private String jdbcUrl;
        private String username;
        private String password;

        public DeleteRecordsFromDestinationTransform(String jdbcUrl, String username, String password) {
            this.jdbcUrl = jdbcUrl;
            this.username = username;
            this.password = password;
        }

//        DeleteRecordsFromDestinationTransform que estende PTransform<PBegin, PCollection<Void>>. Essa classe encapsula
//        o passo de exclusão em um "Composite PTransform".
//
//        Dessa forma, a pipeline irá executar as etapas 2, 3 e 4 em paralelo, mantendo o paralelismo entre essas etapas.
//        O passo de exclusão será executado antes dessas etapas de forma sequencial dentro do Composite PTransform.
//        O paralelismo será mantido entre as etapas 2, 3 e 4, mas o passo de exclusão irá ocorrer apenas uma vez no início
//        da pipeline.
//
//        Essa abordagem preserva o paralelismo e permite que a pipeline seja mais eficiente em termos de tempo de execução,
//        enquanto garante que a exclusão seja concluída antes de prosseguir com as etapas subsequentes.
        @Override
        public PCollection<Void> expand(PBegin input) {
            return input.apply("CreateEmptyInputJustForDeletion", Create.of((Void) null))
                    .apply("DeleteRecordsFromDestination", ParDo.of(new DeleteRecordsFromDestinationFn(jdbcUrl, username, password)));
        }
    }
}
