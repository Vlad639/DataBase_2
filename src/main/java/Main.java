import java.io.BufferedReader;
import java.io.FileReader;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Date;
import java.util.Properties;

public class Main {
    private static Connection connection;

    private static Connection newConnection() throws SQLException {
        String url = "jdbc:postgresql://localhost/JDBS_study";
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

    private static void createTables() throws SQLException {
        executeSqlQueryFromFIle("src/main/resources/create_tables.sql");
    }

    private static void fillTables() throws SQLException {
        executeSqlQueryFromFIle("src/main/resources/fill_all_tables.sql");
    }

    private static void showResidentsFromResultSet(ResultSet resultSet) throws SQLException {
        if (!resultSet.isBeforeFirst()) {
            System.out.println("Пустой resultSet");
            return;
        }

        while (resultSet.next()){
            BigDecimal passportNumber = resultSet.getBigDecimal("passport_number");
            String secondName = resultSet.getString("second_name");
            String firstName = resultSet.getString("first_name");
            String lastName = resultSet.getString("last_name");
            Date bornDate = resultSet.getDate("born_date");
            System.out.print(passportNumber+" | ");
            System.out.format("%15s | %15s |%15s | %10s |\n", secondName, firstName, lastName, bornDate);
        }
    }

    private static void showHumansInCertainCity(int cityID) throws SQLException {
        executeSQLandShowResult(
                "SELECT public.\"Humans\".* FROM public.\"Humans\", public.\"Residents\", public.\"Cities\", public.\"Streets\", public.\"Houses\", public.\"Flats\"\n" +
                        "WHERE \n" +
                        "\tpassport_number = human_link AND \n" +
                        "\tflat_link = flat_id AND \n" +
                        "\thouse_link = house_id AND \n" +
                        "\tstreet_link = street_id AND\n" +
                        "\tcity_link = city_id AND\n" +
                        "\tcity_id = ?;\n",
                cityID);
    }

    private static void showHumansInCertainFlat(int flatID) throws SQLException {
        executeSQLandShowResult("SELECT public.\"Humans\".* FROM public.\"Humans\", public.\"Residents\"\n" +
                        "WHERE \n" +
                        "\tpassport_number = human_link AND flat_link = ?;\n",
                flatID);
    }

    private static void showHumansInCertainHouse(int houseID) throws SQLException {
        executeSQLandShowResult("SELECT public.\"Humans\".* FROM public.\"Humans\", public.\"Residents\", public.\"Houses\", public.\"Flats\"\n" +
                "WHERE \n" +
                "\tpassport_number = human_link AND (flat_link = flat_id) AND (house_link = house_id) AND house_id = ?\n",
                houseID);
    }

    private static void showHumansFromStreetList(int[] streetList) throws SQLException {
        StringBuilder streetListFromIN = new StringBuilder("(");
        for (int elem: streetList) {
            streetListFromIN.append(elem).append(", ");
        }
        streetListFromIN.append("!");
        streetListFromIN = new StringBuilder(streetListFromIN.toString().replace(", !", ")"));

        String query = "SELECT public.\"Humans\".* FROM public.\"Humans\", public.\"Residents\", public.\"Streets\", public.\"Houses\", public.\"Flats\"\n" +
                "WHERE\n" +
                "\tpassport_number = human_link AND \n" +
                "\tflat_link = flat_id AND \n" +
                "\thouse_link = house_id AND \n" +
                "\tstreet_link = street_id AND\n" +
                "\tstreet_id IN "+streetListFromIN+"\n";

        PreparedStatement preparedStatement = connection.prepareStatement(query);
        preparedStatement.execute();
        ResultSet resultSet = preparedStatement.getResultSet();
        showResidentsFromResultSet(resultSet);
    }

    private static void changesFlatsResidents(int firstFlatID, int secondFlatID) throws SQLException {
        String query = "CREATE TABLE public.\"Temp_variables\" (\n" +
                "\t\"flat_1\" integer NOT NULL,\n" +
                "\t\"flat_2\" integer NOT NULL\n" +
                ");\n" +
                "INSERT INTO public.\"Temp_variables\"(flat_1, flat_2)\n" +
                "VALUES(?, ?); --Какие квартиры обменять жильцами\n" +
                "\n" +
                "CREATE TABLE public.\"Temp_humans_ids_from_2_flat\" AS\n" +
                "SELECT human_link FROM public.\"Residents\"\n" +
                "WHERE\n" +
                "\tflat_link = (SELECT flat_2 FROM public.\"Temp_variables\");\n" +
                "\n" +
                "UPDATE public.\"Residents\"\n" +
                "SET flat_link = (SELECT flat_2 FROM public.\"Temp_variables\") \n" +
                "WHERE flat_link = (SELECT flat_1 FROM public.\"Temp_variables\");\n" +
                "\n" +
                "UPDATE public.\"Residents\"\n" +
                "SET flat_link = (SELECT flat_1 FROM public.\"Temp_variables\") \n" +
                "WHERE human_link IN (SELECT human_link FROM public.\"Temp_humans_ids_from_2_flat\");\n" +
                "\n" +
                "DROP TABLE IF EXISTS public.\"Temp_variables\";\n" +
                "DROP TABLE IF EXISTS public.\"Temp_humans_ids_from_2_flat\";\n";
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        preparedStatement.setInt(1, firstFlatID);
        preparedStatement.setInt(2, secondFlatID);
        preparedStatement.execute();
    }

    private static void moveResidentsToNewFlat(int oldFlat, int newFlat) throws SQLException {
        String query = "CREATE TABLE public.\"Temp_variables\" (\n" +
                "\t\"old_flat\" integer NOT NULL,\n" +
                "\t\"new_flat\" integer NOT NULL\n" +
                ");\n" +
                "INSERT INTO public.\"Temp_variables\"(old_flat, new_flat)\n" +
                "VALUES(?, ?); \n" +
                "\n" +
                "INSERT INTO public.\"Residents\" (human_link, flat_link)\n" +
                "SELECT human_link, new_flat FROM public.\"Residents\", public.\"Temp_variables\"\n" +
                "WHERE flat_link = (SELECT old_flat FROM public.\"Temp_variables\");\n" +
                "\n" +
                "DELETE FROM public.\"Residents\"\n" +
                "WHERE flat_link = (SELECT old_flat FROM public.\"Temp_variables\");\n" +
                "\n" +
                "DROP TABLE IF EXISTS public.\"Temp_variables\";\n";
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        preparedStatement.setInt(1, oldFlat);
        preparedStatement.setInt(2, newFlat);
        preparedStatement.execute();
    }

    private static void deleteHumanFromFlat(int passportNumber) throws SQLException {
        String query = "DELETE FROM public.\"Residents\"\n" +
                "WHERE human_link = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        preparedStatement.setInt(1, passportNumber);
        preparedStatement.execute();
    }

    private static void showFlatOwners(int flatID) throws SQLException {
        executeSQLandShowResult(
                "SELECT public.\"Humans\".* FROM public.\"Humans\", public.\"Flats\", public.\"Flats_owners\"\n" +
                        "WHERE\n" +
                        "(passport_number = human_link) AND (flat_link = flat_id) AND flat_id = ?",
                flatID) ;
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

    public static void main(String[] args) throws SQLException {
        connection = newConnection();

        createTables();
        fillTables();
        showHumansInCertainCity(2);
       // showHumansInCertainFlat(42);
        //showHumansInCertainHouse(3);
        //showHumansFromStreetList(new int[]{7, 9});
        //showFlatOwners(1);
        //changesFlatsResidents(2,42);


        connection.close();

    }
}
