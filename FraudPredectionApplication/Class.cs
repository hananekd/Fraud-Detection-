using Microsoft.Data.Analysis;


public class fraud_reasons
{
    public static DataFrame is_PIN_Fraud(DataFrame data)
    {
        // Set the 'is_PIN_fraud' column to false for all rows
        List<bool> is_PIN_fraud = new List<bool>();
        // Set 'is_PIN_fraud' to true where 'n_PIN_attempts' is not equal to 1
        var n_PIN_attemptsColumn = data.Columns["n_PIN_attempts"];
        foreach (int pin_attem in n_PIN_attemptsColumn)
        {
            //Console.WriteLine(pin_attem);
            if (pin_attem != 1)
            {
                
                is_PIN_fraud.Add(true);
            }
            else
            {
                is_PIN_fraud.Add(false);
            }
        }
        var arrColumn = new BooleanDataFrameColumn("is_PIN_fraud", is_PIN_fraud);
        data.Columns.Add(arrColumn);

        foreach (var m in data.Columns["is_PIN_fraud"])
        {
            Console.WriteLine(m);
        }
        return data ;
    }



    public static DataFrame is_RC_Fraud(DataFrame data)
    {
        // Set the 'is_PIN_fraud' column to false for all rows
        List<bool> is_RC_fraud = new List<bool>();
        // Set 'is_PIN_fraud' to true where 'n_PIN_attempts' is not equal to 1
        var status = data.Columns["Statut"];
        foreach (int stat in status)
        {
            Console.WriteLine(stat);
            if (stat == 1)
            {

                is_RC_fraud.Add(true);
            }
            else
            {
                is_RC_fraud.Add(false);
            }
        }
        var arrColumn = new BooleanDataFrameColumn("is_RC_fraud", is_RC_fraud);
        data.Columns.Add(arrColumn);

        foreach (var m in data.Columns["is_RC_fraud"])
        {
            Console.WriteLine(m);
        }
        return data;
    }
}



