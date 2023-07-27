package org.example;

import org.apache.beam.sdk.transforms.DoFn;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DeleteRecordsFromDestinationFn extends DoFn<Void, Void> {

    private String jdbcUrl;
    private String username;
    private String password;

    public DeleteRecordsFromDestinationFn(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }


    @ProcessElement
    public void processElement(ProcessContext c) {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            // Estabelece a conexão com o banco de dados de destino
            System.out.println("Conexão com o banco de dados estabelecida.");

            // Cria um objeto Statement para executar o comando SQL
            Statement statement = connection.createStatement();

            // Executa o comando de exclusão de todos os registros na tabela de destino
            String deleteQuery = "DELETE FROM dados";
            int rowsDeleted = statement.executeUpdate(deleteQuery);

            System.out.println("Registros excluídos: " + rowsDeleted);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
