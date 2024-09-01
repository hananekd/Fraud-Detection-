import joblib
import numpy as np
import sys
import json
# Load the ONNX model
if __name__ == "__main__":
    model_path = "D:/Program Files/WebApplication2/WebApplication2/Fraud_reason_model.pkl"
    model = joblib.load(model_path)

    try:

        json_array = sys.argv[1]
        input_array = np.array(json.loads(json_array))
        test_float = input_array.astype('float32')
        input_tensor = np.reshape(test_float, (1, -1))
        predicted_proba = model.predict_proba(input_tensor)
        predicted_reasons = model.predict(input_tensor)
        reasons = ['is_PIN_fraud','is_HM_fraud','is_RC_fraud','is_IF_fraud','is_CVM_fraud','is_unhour_fraud','is_expDate_fraud']
        i=0
        for predicted_class  in predicted_proba:
            print("the reason :"+reasons[i]+" has these probabilities :"+str(predicted_class[0])+" so the response is :"+str(predicted_reasons[0][i]))
            i+=1

        

    except (ValueError, json.JSONDecodeError):
        print("Error: Invalid input. Please provide a valid JSON array.")
        sys.exit(1)

 
