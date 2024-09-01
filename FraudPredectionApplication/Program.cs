using Microsoft.ML;
using Microsoft;
using Microsoft.Data.Analysis;
using Microsoft.ML.Data;
using System.Data.SqlClient;
using System.Collections;
using Apache.Arrow;
using System.Data;
using System;

using System.Linq;
using System.Globalization;
using System.Runtime.Intrinsics.Arm;
using Apache.Arrow.Types;
using System.Diagnostics;
using System.Text.RegularExpressions;

using System.Text;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;


using System.IO;
using Microsoft.AspNetCore.Hosting;

using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Numerics;
using WebApplication2;

public class HouseData
{
    public DateTime TransactionDate { get; set; }
    public int TransactionNumber { get; set; }
    public string CardNumber { get; set; }
    public string Type { get; set; }
    public int Amount { get; set; }
    public int Statut { get; set; }
    public bool isCashPresented { get; set; }
    public bool isCashTaken { get; set; }
    public bool isCashRetracted { get; set; }
    public bool isCashoutError { get; set; }
    public bool ExistInHost { get; set; }
    public bool IsRejected { get; set; }
}

public class HouseData1
{
    public int Id { get; set; }
    public String Name { get; set; }
    public string Description { get; set; }

}

public class HouseData2
{
    public String Id { get; set; }
    public String Organisation { get; set; }

}



class Program
{
    public static DataFrame Connection<T>(string command) where T : class
    {
        MLContext mlContext = new MLContext();

        DatabaseLoader loader = mlContext.Data.CreateDatabaseLoader<T>();
        string connectionString = @"Data Source=DESKTOP-01AI7H3;Initial Catalog=backup;Integrated Security=True;";

        DatabaseSource dbSource = new DatabaseSource(SqlClientFactory.Instance, connectionString, command);

        IDataView data = loader.Load(dbSource);
        DataFrame dataframe = data.ToDataFrame(-1);
        return dataframe;

    }
    public static DataFrame import_avtrans_data()
    {
        // import av transaction data
        string cmdAvdata = "SELECT TOP(1000)[AtmID],[TransactionDate],[TransactionNumber],[CardNumber],[Type],[Amount],[Statut],[isCashPresented],[isCashTaken],[isCashRetracted],[isCashoutError],[ExistInHost],[IsRejected] FROM AVTransaction where CardNumber is not null ; ";
        DataFrame dataframe = Connection<HouseData>(cmdAvdata);
        DataFrame avtransactions = dataframe.DropNulls();
        long rows = avtransactions.Rows.Count;
        var indexes = Enumerable.Range(1, (int)rows).Select(x => (int)x).ToArray();
        var IndexColumn = new Int32DataFrameColumn("indexes", indexes);
        avtransactions.Columns.Add(IndexColumn);

        
        /*        Console.WriteLine(rows);
                for (int i =0; i < rows; i++)
                {
                    Console.WriteLine(avtransactions.Rows.ElementAt(i));
                }*/
        //Console.WriteLine($"Number of rows in av transactions: {rows}");
        //Console.WriteLine(avtransactions);


        // import atmcities data
        string AtmsCitiesCommand = "SELECT * FROM AtmsCities";
        DataFrame Atmecities = Connection<HouseData1>(AtmsCitiesCommand);
        long rows1 = Atmecities.Rows.Count;
        //Console.WriteLine($"Number of rows in AtmsCities data: {rows1}");
        //Console.WriteLine(Atmecities);


        // import atms data
        string AtmsCommand = "SELECT [Id] ,[Organisation] FROM ATms";
        DataFrame Atms = Connection<HouseData2>(AtmsCommand);
        long rows2 = Atms.Rows.Count;
        //Console.WriteLine($"Number of rows in Atms data: {rows2}");
        //Console.WriteLine(Atms);


        //sort by card number 
        avtransactions = avtransactions.OrderBy("CardNumber");


        //sort by datetime 
        var grouped = from card in avtransactions.Rows.Cast<DataFrameRow>()
                      group card by card[2] into cardGroup
                      select new
                      {
                          CardNumber = cardGroup.Key,
                          Rows = cardGroup.OrderByDescending(row => row[0])
                      };
        //Console.WriteLine(avtransactions.Info());
        string[] columnNames = avtransactions.Columns.Select(col => col.Name).ToArray();
        DataFrameColumn[] newColumns = new DataFrameColumn[columnNames.Length];

        for(int i = 0; i < newColumns.Length; i++)
{
            Type columnType = avtransactions.Columns[i].DataType;
            if (columnType == typeof(int))
            {
                newColumns[i] = new Int32DataFrameColumn(columnNames[i], avtransactions.Rows.Count);
            }
            else if (columnType == typeof(DateTime))
            {
                newColumns[i] = new DateTimeDataFrameColumn(columnNames[i], avtransactions.Rows.Count);
            }
            else if (columnType == typeof(string))
            {
                newColumns[i] = new StringDataFrameColumn(columnNames[i], avtransactions.Rows.Count);
            }
            else if (columnType == typeof(bool))
            {
                newColumns[i] = new BooleanDataFrameColumn(columnNames[i], avtransactions.Rows.Count);
            }
            // Add more conditions for other column types as needed
            else
            {
                // Handle unsupported column types or add more cases as needed
                throw new NotSupportedException($"Unsupported column type: {columnType}");
            }

            //Console.WriteLine(columnType);
        }


        DataFrame newDataFrame = new DataFrame(newColumns);

        int rowIndex = 0;
        foreach (var group in grouped)
        {
            foreach (var row in group.Rows)
            {
                for (int i = 0; i < row.Count(); i++)
                {
                    newDataFrame[rowIndex, i] = row[i];
                }
                rowIndex++;
            }
        }

       

        return newDataFrame;
    }


    public static DataFrame time_between_trans(DataFrame data)
    {
        DataFrameColumn uniquecards = data.GroupBy("CardNumber").Sum("Amount").Columns[0];
        var cardColumn = data.Columns["CardNumber"];
        var Transactiondates = data.Columns["TransactionDate"];
        DateTime previousTransactionDate = (DateTime)Transactiondates[0];
        List<double> timeBetweenTransactions = new List<double>();
        //Console.WriteLine(uniquecards);
        int i = 0;
        int j = 0;
        var card_ = uniquecards[i];
        Console.WriteLine(uniquecards.Length);
        Console.WriteLine(cardColumn.Length);

        foreach (var card in cardColumn)
        {

            DateTime transactionDate = (DateTime)Transactiondates[j];

            if (card.Equals(card_))
            {
                double timeDifferenceInMinites = (transactionDate - previousTransactionDate).TotalMinutes;
                timeBetweenTransactions.Add(timeDifferenceInMinites);
                previousTransactionDate = transactionDate;
                //Console.WriteLine(timeDifferenceInMinites);
            }

            else
            {
                //Console.WriteLine(i);
                timeBetweenTransactions.Add(0);
                i++;
                previousTransactionDate = transactionDate;
                card_ = uniquecards[i]; // Update card_
               
            }
            j++;



        }
        Console.WriteLine("done");

        var Time_diff = new DoubleDataFrameColumn("time_between_transactions", timeBetweenTransactions);
        data.Columns.Add(Time_diff);

        /*foreach(var t in data.Columns["time_between_transactions"])
        {
            Console.WriteLine(t);
        }*/
        //Console.WriteLine(data.Head(6));

        return data;
    }

    public static DataFrame velocity_per_hour(DataFrame data)
    {
        var time_diff = data.Columns["time_between_transactions"];
        List<double> velocity = new List<double>();

        foreach (double t in time_diff)
        {
            double hours = t / 60; // Convert minutes to hours
            double v = hours == 0 ? 0 : 1.0 / hours; // Calculate velocity per hour
            velocity.Add(v);
        }

        var Velocity = new DoubleDataFrameColumn("frequecy_per_hour", velocity);
        data.Columns.Add(Velocity);

        return data;
    }



    public static DataFrame AddRandomPINAttempts(DataFrame data)
    {

        //data.Columns.Add(new PrimitiveDataFrameColumn<int>("n_PIN_attempts", 1));
        List<int> PINAttempts = Enumerable.Range(2, 4).ToList();
        int datasetLength = (int)data.Rows.Count;

        // Create a Random object to generate random numbers
        Random random = new Random();
        int numElements = (int)(0.03 * datasetLength);
        // Select 'numElements' random indices from the dataset
        List<int> randomIndices = Enumerable.Range(0, datasetLength)
            .OrderBy(_ => random.Next())
            .Take(numElements)
            .ToList();
        //Console.WriteLine(randomIndices.Count);
        List<int> PINAttempts_ = new List<int>();
        for (int i = 0; i < datasetLength; i++)
        {
            if (randomIndices.Contains(i))
            {
                PINAttempts_.Add(random.Next(PINAttempts.Count));
            }
            else
            {
                PINAttempts_.Add(1);
            }
        }

        var PIN = new Int32DataFrameColumn("n_PIN_attempts", PINAttempts_);
        data.Columns.Add(PIN);


        /*foreach (var m in data.Columns["n_PIN_attempts"])
        {
            Console.WriteLine(m);
        }
        Console.WriteLine(data.Head(6));*/

        return data;
    }


    public static DataFrame cvm_methods(DataFrame data)
    {
        List<int> cvm_methods = new List<int> { 1, 2, 5, 8, 80 };

        int datasetLength = (int)data.Rows.Count;

        // Create a Random object to generate random numbers
        Random random = new Random();
        int numElements = (int)(0.04 * datasetLength);
        // Select 'numElements' random indices from the dataset
        List<int> randomIndices = Enumerable.Range(0, datasetLength)
            .OrderBy(_ => random.Next())
            .Take(numElements)
            .ToList();
        List<int> cvms_methods = new List<int>();
        for (int i = 0; i < datasetLength; i++)
        {
            if (randomIndices.Contains(i))
            {
                cvms_methods.Add(random.Next(cvm_methods.Count));
            }
            else
            {
                cvms_methods.Add(3);
            }
        }

        var CVM = new Int32DataFrameColumn("cvm_methods", cvms_methods);
        data.Columns.Add(CVM);


        /*foreach (var m in data.Columns["cvm_methods"])
        {
            Console.WriteLine(m);
        }
        Console.WriteLine(data.Head(6));*/

        return data;

    }


    public static DataFrame issuer_city(DataFrame data)
    {
        int datasetLength = (int)data.Rows.Count;


        // odd issuer cities
        List<string> ODD_cities = new List<string> { "Marrakech", "El ayoun", "Dakhla", "Agadir" };
        // Create a Random object to generate random numbers
        Random random1 = new Random();
        int numElements1 = (int)(0.03 * datasetLength);
        // Select 'numElements' random indices from the dataset
        List<int> randomIndices1 = Enumerable.Range(0, datasetLength)
            .OrderBy(_ => random1.Next())
            .Take(numElements1)
            .ToList();


        // normal issuer cities 
        List<string> normal_cities = new List<string> { "Casablanca", "Rabat", "El mohammadia" };
        // Create a Random object to generate random numbers
        Random random2 = new Random();
        int numElements2 = datasetLength - numElements1;
        // Select 'numElements' random indices from the dataset
        List<int> randomIndices2 = new List<int>();
        while (randomIndices2.Count < numElements2)
        {
            int randomIndex = random2.Next(datasetLength);
            if (!randomIndices1.Contains(randomIndex) && !randomIndices2.Contains(randomIndex))
            {
                randomIndices2.Add(randomIndex);
            }
        }


        List<string> issuer_City = new List<string>();
        for (int i = 0; i < datasetLength; i++)
        {
            if (randomIndices1.Contains(i))
            {
                issuer_City.Add(ODD_cities[random1.Next(ODD_cities.Count)]);
            }
            else if (randomIndices2.Contains(i))
            {
                issuer_City.Add(normal_cities[random2.Next(normal_cities.Count)]);
            }
        }


        var issuerCities = new StringDataFrameColumn("Issuer_cities", issuer_City);
        data.Columns.Add(issuerCities);


        /*foreach (var m in data.Columns["Issuer_cities"])
        {
            Console.WriteLine(m);
        }
        Console.WriteLine(data.Head(6));*/

        return data;
    }

    public static DataFrame experation_date(DataFrame data) {

        Random random = new Random();
        var transactionTime = data.Columns["TransactionDate"];
        List<double> expiryDiffrence = new List<double>();
        List<DateTime> ExpiryDate = new List<DateTime>();
        foreach (var row in  transactionTime)
        {
            DateTime transactionDate = (DateTime)row;
            DateTime newExpiryDate;
            double expDiff;
            int randomY  = random.Next(1, 4);
            int randomM = random.Next(1, 13);
            int randomD = random.Next(1, 30);
            int randomH = random.Next(0, 23);
            int randomMi = random.Next(0,59);
            int randomS = random.Next(0, 59);

            if (random.NextDouble() < 0.11) // 11% chance of generating an invalid expiry date
            {
                

                newExpiryDate = transactionDate.AddYears(randomY).AddMonths(randomM).AddDays(randomD).AddHours(randomH).AddMinutes(randomMi).AddSeconds(randomS) ; // Subtract one year from transaction date
                                                                                                                                                                   // Update the value in the DataTable if needed
                expiryDiffrence.Add((transactionDate - newExpiryDate).TotalMinutes);


            }
            else
            {
                newExpiryDate = transactionDate.AddYears(-randomY).AddMonths(-randomM).AddDays(-randomD).AddHours(-randomH).AddMinutes(-randomMi).AddSeconds(-randomS); // Subtract one year from transaction date
                expiryDiffrence.Add((transactionDate - newExpiryDate).TotalMinutes);

            }
            ExpiryDate.Add(newExpiryDate);
        }
        var ExpiryDates = new DateTimeDataFrameColumn("ExpiryDate", ExpiryDate);
        data.Columns.Add(ExpiryDates);

        var Expirydiff = new DoubleDataFrameColumn("ExpiryDifference", expiryDiffrence);
        data.Columns.Add(Expirydiff);
        return data;


    }

    public static DataFrame scale_exp(DataFrame data)
    {
        var transactiondiff = data.Columns["ExpiryDifference"];
        double scalingFactor = 2.0;
        List<double> scaleddiff = new List<double>();
        foreach (double diff in transactiondiff)
        {
            scaleddiff.Add(diff >= 0 ? diff * scalingFactor : diff / scalingFactor);

        }

        var Expirydiff_ = new DoubleDataFrameColumn("ExpiryDifference_scaled", scaleddiff);
        data.Columns.Add(Expirydiff_);
        data.Columns.Remove("ExpiryDifference");

        return data;


    }

    public static DataFrame Label_Encoding(DataFrame data, string columnName)
    {
        Dictionary<string, int> labelMapping = new Dictionary<string, int>();
        int labelCount = 0;
        var column = data.Columns[columnName];
        List<int> encoding = new List<int>();
        int i = 0;
        foreach (var item in column)
        {

            string category = (string)item;

            if (!labelMapping.ContainsKey(category))
            {
                labelMapping[category] = labelCount;
                labelCount++;
            }

            encoding.Add(labelMapping[category]);
        }

        /*foreach(var item in encoding)
        {
            Console.WriteLine(item);
        }*/

        var arrColumn = new Int32DataFrameColumn(columnName + "_encoded", encoding);
        data.Columns.Add(arrColumn);
        data.Columns.Remove(columnName);

        return data;


    }


    public static DataFrame data_scalingI(DataFrame data, string col)
    {
        var column = data.Columns[col];
        double mean = column.Mean();
        double std = 0;
        List<double> scaled_values = new List<double>();
        foreach (int v in column)
        {
            double vv = (double)v;
            std += (vv - mean) * (vv - mean);
        }
        std = Math.Sqrt(std / (column.Length - 1));

        foreach (int col_ in column)
        {
            if (std != 0)
            {
                double col__ = (double)col_;
                double scaledValue = (col__ - mean) / std;
                scaled_values.Add(scaledValue);
            }
            else
            {
                scaled_values.Add(0);
            }

        }
        var arrColumn = new DoubleDataFrameColumn(col + "_scaled", scaled_values);
        data.Columns.Add(arrColumn);
        data.Columns.Remove(col);

        return data;
    }



    public static DataFrame data_scalingD(DataFrame data, string col)
    {
        var column = data.Columns[col];
        double mean = column.Mean();
        double std = 0;
        List<double> scaled_values = new List<double>();
        foreach (double v in column)
        {
            double vv = (double)v;
            std += (vv - mean) * (vv - mean);
        }
        std = Math.Sqrt(std / (column.Length - 1));

        foreach (double col_ in column)
        {
            double col__ = (double)col_;
            double scaledValue = (col__ - mean) / std;
            scaled_values.Add(scaledValue);
        }
        var arrColumn = new DoubleDataFrameColumn(col + "_scaled", scaled_values);
        data.Columns.Add(arrColumn);
        data.Columns.Remove(col);

        return data;
    }

   
    public static DataFrame convert_boolToInt(DataFrame data, string col)
    {
        var column = data.Columns[col];
        List<double> boolvalues = new List<double>();
        foreach (var item in column)
        {
            var boolValue = (bool)item;
            int intValue = boolValue ? 1 : 0;
            boolvalues.Add(intValue);
        }



        var arrColumn = new DoubleDataFrameColumn(col + "_toInt", boolvalues);
        data.Columns.Add(arrColumn);
        data.Columns.Remove(col);

        return data;
    }

    public static DataFrame calcule_features()
    {
        DataFrame data = import_avtrans_data();
        data = time_between_trans(data);
        data = velocity_per_hour(data);
        data = AddRandomPINAttempts(data);
        data = cvm_methods(data);
        data = issuer_city(data);
        data = experation_date(data);
        data = scale_exp(data);
        //data = moyenne(data);


        //Console.WriteLine("before encoding");
        /*foreach (var col in data.Columns)
        {
            Console.WriteLine(col.Name);
        }*/
        //Console.WriteLine(data.Info());

        //encoding the data
        data = Label_Encoding(data, "Issuer_cities");
        data = Label_Encoding(data, "CardNumber");
        data = Label_Encoding(data, "Type");


        //Console.WriteLine("after encoding");
        /*foreach (var col in data.Columns)
        {
            Console.WriteLine(col.Name);
        }*/
        //Console.WriteLine(data.Info());

        /*_____________________________________________________________
        data = fraud_reasons.is_PIN_Fraud(data);
        data = fraud_reasons.is_RC_Fraud(data);
        _____________________________________________________________*/


        //scaling the data
        string[] columnNamesI = new string[]
        {
            "TransactionNumber",
            "Type_encoded",
            "Amount",
            "Statut",
            "CardNumber_encoded",
            "n_PIN_attempts",
            "cvm_methods",
            "Issuer_cities_encoded"
        };

        string[] columnNamesD = new string[]
        {
            "time_between_transactions",
            "frequecy_per_hour"
        };
        foreach (string column in columnNamesI)
        {
            data = data_scalingI(data, column);
            //Console.WriteLine(column);
        }

        foreach (string column in columnNamesD)
        {
            data = data_scalingD(data, column);
            //Console.WriteLine(column);
        }
        data.Columns.Remove("TransactionNumber_scaled");
        //Console.WriteLine(data.Head(5));
        //Console.WriteLine("____________________");
        //Console.WriteLine(data.Columns.Count);
        //PCA(data);
        string[] columnNamesB = new string[]
        {
            "isCashPresented",
            "isCashTaken",
            "isCashRetracted",
            "isCashoutError",
            "ExistInHost"
        };

        foreach (string column in columnNamesB)
        {
            data = convert_boolToInt(data, column);
            //Console.WriteLine(column);
        }
        return data;
    }


    public static string predictfraud(DataFrame data, int index)
    {
        //input data
        var indexes = data.Columns["indexes"];
        int row_index = 0;
        for(int i=0; i < indexes.Length; i++)
        {
            int num = (int)indexes[i];
            if (index == num)
            {
                row_index = i;
                break;
            }
        }
        data.Columns.Remove("indexes");
        var data1 = data.Rows[row_index];
        float[] rowArray = new float[] { };

        foreach (double val in data1)
        {
            float v = (float)val;
            rowArray = rowArray.Append(v).ToArray();

        }

        string inputJson = Newtonsoft.Json.JsonConvert.SerializeObject(rowArray);

        

        var psi = new ProcessStartInfo();
        psi.FileName = "C:/Program Files/Python310/python.exe";

        var script = "D:/Program Files/WebApplication2/WebApplication2/predictions.py";
        
        psi.Arguments = $"\"{script}\" \"{inputJson}\"";
        psi.UseShellExecute = false;
        psi.CreateNoWindow = true;
        psi.RedirectStandardOutput = true;
        psi.RedirectStandardError = true;

        var errors = "";
        var results = "";

        using (var process = System.Diagnostics.Process.Start(psi))
        {
            errors = process.StandardError.ReadToEnd();
            results = process.StandardOutput.ReadToEnd();

        }
        Console.WriteLine(errors);
        Console.WriteLine(results);
        return results;
    }

    public static string predictfraud_reasons(DataFrame data, int i)
    {
        //input data

        var data1 = data.Rows[i];
        float[] rowArray = new float[] { };

        foreach (double val in data1)
        {
            float v = (float)val;
            rowArray = rowArray.Append(v).ToArray();

        }

        string inputJson = Newtonsoft.Json.JsonConvert.SerializeObject(rowArray);



        var psi = new ProcessStartInfo();
        psi.FileName = "C:/Program Files/Python310/python.exe";

        var script = "D:/Program Files/WebApplication2/WebApplication2/fraud_reason_predection.py";

        psi.Arguments = $"\"{script}\" \"{inputJson}\"";
        psi.UseShellExecute = false;
        psi.CreateNoWindow = true;
        psi.RedirectStandardOutput = true;
        psi.RedirectStandardError = true;

        var errors = "";
        var results = "";

        using (var process = System.Diagnostics.Process.Start(psi))
        {
            errors = process.StandardError.ReadToEnd();
            results = process.StandardOutput.ReadToEnd();

        }
        Console.WriteLine(errors);
        Console.WriteLine(results);
        return results;
    }

    public static string predictions()
    {
        DataFrame data = calcule_features();
        data.Columns.Remove("TransactionDate");
        data.Columns.Remove("ExpiryDate");
        data.Columns.Remove("IsRejected");
        foreach (var col in data.Columns)
        {
            Console.WriteLine(col.Name);
        }
        Console.WriteLine(data.Info());
        //string[] results = new string[] { };
        /*for(int j=0; j < 10; j++)
        {
            results.Append(predictpython(data, j));
        }*/
        //results.Append(predictpython(data, 9));
        string resultclassification = predictfraud(data, 30);

        String result_fraudreason = predictfraud_reasons(data, 30);
        return resultclassification+ "\n\n"+ result_fraudreason;
    }
    static void Main(string[] args)
    {

        
        var builder = WebApplication.CreateBuilder(args);

        // ConfigureServices is where you configure services for your application
        builder.Services.AddRazorPages();

        var app = builder.Build();

        if (app.Environment.IsDevelopment())
        {
            app.UseDeveloperExceptionPage();
        }
        else
        {
            app.UseExceptionHandler("/Home/Error");
        }
        //string[] Results = predictions();
        /*for (int i=0; i< Results.Length; i++)
        {
            int index = i; // Capture the current value of 'i' in a local variable
            app.MapGet($"/result-{index}", context => context.Response.WriteAsync(Results[index]));

        }*/
        app.MapGet("/", predictions);
        app.Run();

    }
}
