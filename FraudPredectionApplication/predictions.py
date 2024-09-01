import joblib
import numpy as np
import sys
import json
# Load the ONNX model
if __name__ == "__main__":
    model_path = "D:/Program Files/WebApplication2/WebApplication2/fraud_classification_model.pkl"
    model = joblib.load(model_path)

    try:

        json_array = sys.argv[1]
        input_array = np.array(json.loads(json_array))[:-1]
        test_float = input_array.astype('float32')
        input_tensor = np.reshape(test_float, (1, -1))
        predicted_proba = model.predict_proba(input_tensor)
        normal_prob=predicted_proba[0][0]
        fraud_prob =predicted_proba[0][1]

        print("transaction is valid with probability of :",normal_prob)
        print("transaction is fraud with probability of :",fraud_prob)
        
        if normal_prob>fraud_prob:
            print("so the transaction is normal")
        else:
            print("so the transaction is fraudulent")

    except (ValueError, json.JSONDecodeError):
        print("Error: Invalid input. Please provide a valid JSON array.")
        sys.exit(1)

 
