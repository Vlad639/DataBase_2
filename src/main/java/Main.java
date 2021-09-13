import java.io.BufferedReader;
import java.io.FileReader;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Date;
import java.util.Properties;

public class Main {
    private static Connection connection;

    private static Connection newConnection() throws SQLException {
        String url = "jdbc:postgresql://localhost/JDBC_study";
        Properties properties = new Properties();
        properties.setProperty("user", "postgres");
        properties.setProperty("password", "qwerty123");
        return DriverManager.getConnection(url, properties);
    }

    private static int executeSQL(String query) throws SQLException {
        Statement statement = connection.createStatement();
        return statement.executeUpdate(query);
    }

    private static String getFileContent(String fileName){
        String line;
        StringBuilder fileContent = new StringBuilder();

        try(FileReader fileReader = new FileReader(fileName);
            BufferedReader reader = new BufferedReader(fileReader)){
            line  = reader.readLine();
            while (line != null){
                fileContent.append(line).append("\n");
                line = reader.readLine();
            }
            return fileContent.toString();
        }
        catch(Exception e){
            e.printStackTrace();
        }
        return "";
    }

    private static void executeSqlQueryFromFIle(String fileName) throws SQLException {
        String query = getFileContent(fileName);
        executeSQL(query);
    }

    private static void executeSQLandShowResult(String query, int id) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        preparedStatement.setInt(1, id);
        preparedStatement.execute();
        ResultSet resultSet = preparedStatement.getResultSet();
        showResidentsFromResultSet(resultSet);
    }

    private static void showResidentsFromResultSet(ResultSet resultSet) throws SQLException {
        if (!resultSet.isBeforeFirst()) {
            System.out.println("Пустой resultSet");
            return;
        }

        while (resultSet.next()){
            String passportNumber = resultSet.getString("passport_number");
            String secondName = resultSet.getString("second_name");
            String firstName = resultSet.getString("first_name");
            String lastName = resultSet.getString("last_name");
            Date bornDate = resultSet.getDate("born_date");
            System.out.print(passportNumber+" | ");
            System.out.format("%15s | %15s |%15s | %10s |\n", secondName, firstName, lastName, bornDate);
        }
    }

    public static void main(String[] args) throws SQLException {
        connection = newConnection();

        createTables();
        fillTables();

        showHumansInCertainFlat(42);
        //showFlatOwners(1);
        //showHumansInCertainCity(2);
        //showHumansInCertainHouse(3);
        //showHumansFromStreetList(new int[]{7, 9});
        //registerHumanInFlat(1, 4);
        //deleteHumanFromFlat(1);
        //moveResidentsToNewFlat(42, 8);
        //changesFlatsResidents(2,8);


        connection.close();

    }

    private static void createTables() throws SQLException {
        executeSqlQueryFromFIle("src/main/resources/create_tables.sql");
    }

    private static void fillTables() throws SQLException {
        executeSqlQueryFromFIle("src/main/resources/fill_all_tables.sql");
    }

    private static void showHumansInCertainFlat(int flatID) throws SQLException {
        executeSQLandShowResult(
                "SELECT \n" +
                        "\tpublic.\"Humans\".* \n" +
                        "FROM\n" +
                        "\tpublic.\"Humans\"\n" +
                        "\tJOIN public.\"Residents\"\n" +
                        "\t\tON human_id = human_link\n" +
                        "\tWHERE flat_link = ?;",
                flatID);
    }

    private static void showFlatOwners(int flatID) throws SQLException {
        executeSQLandShowResult(
                "SELECT \n" +
                        "\tpublic.\"Humans\".* \n" +
                        "FROM \n" +
                        "\tpublic.\"Humans\"\n" +
                        "\t\n" +
                        "\tJOIN public.\"Flats_owners\"\n" +
                        "\tON human_link = human_id \n" +
                        "WHERE\n" +
                        "\tflat_link = ?;",
                flatID) ;
    }

    private static void showHumansInCertainCity(int cityID) throws SQLException {
        executeSQLandShowResult(
                "SELECT \n" +
                        "\tpublic.\"Humans\".*\n" +
                        "FROM \n" +
                        "\tpublic.\"Streets\"\n" +
                        "\t\n" +
                        "\tJOIN public.\"Houses\"\n" +
                        "\tON street_link = street_id\n" +
                        "\t\n" +
                        "\tJOIN public.\"Flats\"\n" +
                        "\tON house_link = house_id\n" +
                        "\t\n" +
                        "\tJOIN public.\"Residents\"\n" +
                        "\tON flat_link = flat_id\n" +
                        "\t\n" +
                        "\tJOIN public.\"Humans\"\n" +
                        "\tON human_link = human_id\n" +
                        "\t\n" +
                        "\tWHERE city_link = ?;",
                cityID);
    }

    private static void showHumansInCertainHouse(int houseID) throws SQLException {
        executeSQLandShowResult(
                "SELECT \n" +
                        "\tpublic.\"Humans\".*\n" +
                        "FROM \n" +
                        "\tpublic.\"Flats\" \n" +
                        "\t\n" +
                        "\tJOIN public.\"Residents\"\n" +
                        "\tON flat_link = flat_id\n" +
                        "\t\n" +
                        "\tJOIN public.\"Humans\"\n" +
                        "\tON human_link = human_id\n" +
                        "\t\n" +
                        "\tWHERE house_link = ?;",
                houseID);
    }

    private static void showHumansFromStreetList(int[] streetList) throws SQLException {
        StringBuilder streetListFromIN = new StringBuilder("(");
        for (int elem: streetList) {
            streetListFromIN.append(elem).append(", ");
        }
        streetListFromIN.append("!");
        streetListFromIN = new StringBuilder(streetListFromIN.toString().replace(", !", ")"));

        String query = "SELECT \n" +
                "\tpublic.\"Humans\".* \n" +
                "FROM \n" +
                "\tpublic.\"Houses\"\n" +
                "\t\n" +
                "\tJOIN public.\"Flats\"\n" +
                "\tON house_link = house_id\n" +
                "\t\n" +
                "\tJOIN public.\"Residents\"\n" +
                "\tON flat_link = flat_id\n" +
                "\t\n" +
                "\tJOIN public.\"Humans\"\n" +
                "\tON human_link = human_id\n" +
                "\t\n" +
                "\tWHERE street_link in "+streetListFromIN+";";

        PreparedStatement preparedStatement = connection.prepareStatement(query);
        preparedStatement.execute();
        ResultSet resultSet = preparedStatement.getResultSet();
        showResidentsFromResultSet(resultSet);
    }

    private static void registerHumanInFlat(int human_id, int flat_id) throws SQLException {
        String query = "INSERT INTO public.\"Residents\" (human_link, flat_link)\n" +
                "VALUES (?, ?);";
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        preparedStatement.setInt(1, human_id);
        preparedStatement.setInt(2, flat_id);
        preparedStatement.execute();
    }

    private static void deleteHumanFromFlat(int human_id) throws SQLException {
        String query = "DELETE FROM public.\"Residents\"\n" +
                "WHERE human_link = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        preparedStatement.setInt(1, human_id);
        preparedStatement.execute();
    }

    private static void moveResidentsToNewFlat(int oldFlat, int newFlat) throws SQLException {
        String query = "UPDATE public.\"Residents\"\n" +
                "\tSET flat_link = ?\n" +
                "WHERE flat_link = ?;";
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        preparedStatement.setInt(1, newFlat);
        preparedStatement.setInt(2, oldFlat);
        preparedStatement.execute();
    }

    private static void changesFlatsResidents(int firstFlatID, int secondFlatID) throws SQLException {
        String query = "WITH variables AS (SELECT ARRAY[?, ?] AS var_index)\n" +
                "\n" +
                "UPDATE public.\"Residents\"\n" +
                "SET flat_link = \n" +
                "\tCASE\n" +
                "    \tWHEN flat_link = (SELECT var_index[1] FROM variables) THEN (SELECT var_index[2] FROM variables)\n" +
                "\t\tWHEN flat_link = (SELECT var_index[2] FROM variables) THEN (SELECT var_index[1] FROM variables)\n" +
                "    END\n" +
                "WHERE flat_link IN ((SELECT var_index[1] FROM variables), (SELECT var_index[2] FROM variables));";
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        preparedStatement.setInt(1, firstFlatID);
        preparedStatement.setInt(2, secondFlatID);
        preparedStatement.execute();
    }

}
