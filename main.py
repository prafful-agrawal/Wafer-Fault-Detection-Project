from wsgiref import simple_server
from flask import Flask, request, render_template
from flask_cors import CORS, cross_origin
from flask import Response
import flask_monitoringdashboard as dashboard
import os
import json
from training_data_validation_and_insertion import validate_and_insert_training_data
from model_training import train_model
from prediction_data_validation_and_insertion import validate_and_insert_prediction_data
from prediction_from_model import make_predictions

os.putenv('LANG', 'en_US.UTF-8')
os.putenv('LC_ALL', 'en_US.UTF-8')

app = Flask(__name__)
dashboard.bind(app)
CORS(app)


@app.route("/", methods=['GET'])
@cross_origin()
def home():
    return render_template('index.html')


@app.route("/predict", methods=['POST'])
@cross_origin()
def predictRouteClient():
    try:
        if request.json is not None:
            # Extract the path of prediction data
            folder_path = request.json['filepath']
            # Validate and insert the prediction data
            prediction_data_val_ins_obj = validate_and_insert_prediction_data(folder_path)
            prediction_data_val_ins_obj.validate_and_insert_data()
            # Make the predictions for the above data
            prediction_from_model_obj = make_predictions()
            output_path, json_predictions = prediction_from_model_obj.predict()
            # Return the output path and a few predictions
            output_message = 'Prediction output generated at: ' + str(output_path) + '\n' +\
                             'and few of the predictions are: ' + str(json.loads(json_predictions))
            return Response(output_message)
        elif request.form is not None:
            # Extract the path of prediction data
            folder_path = request.form['filepath']
            # Validate and insert the prediction data
            prediction_data_val_ins_obj = validate_and_insert_prediction_data(folder_path)
            prediction_data_val_ins_obj.validate_and_insert_data()
            # Make the predictions for the above data
            prediction_from_model_obj = make_predictions()
            output_path, json_predictions = prediction_from_model_obj.predict()
            # Return the output path and a few predictions
            output_message = 'Prediction output generated at: ' + str(output_path) + '\n' +\
                             'and few of the predictions are: ' + str(json.loads(json_predictions))
            return Response(output_message)
        else:
            print('Nothing Matched!')
    except ValueError:
        return Response('Error occurred! %s' % ValueError)
    except KeyError:
        return Response('Error occurred! %s' % KeyError)
    except Exception as e:
        return Response('Error occurred! %s' % e)


# @app.route("/train", methods=['POST'])
@app.route("/train", methods=['GET'])
@cross_origin()
def trainRouteClient():
    try:
        # if request.json['folderPath'] is not None:
        #     path = request.json['folderPath']

        # Define the path of training data
        folder_path = 'training_batch_files/'
        # Validate and insert the training data
        training_data_val_ins_obj = validate_and_insert_training_data(folder_path)
        training_data_val_ins_obj.validate_and_insert_data()
        # Train the model
        model_training_obj = train_model()
        model_training_obj.train()
        return Response('Training successful!')
    except ValueError:
        return Response('Error occurred! %s' % ValueError)
    except KeyError:
        return Response('Error occurred! %s' % KeyError)
    except Exception as e:
        return Response('Error occurred! %s' % e)


port = int(os.getenv('PORT', 5000))
if __name__ == "__main__":
    host = '0.0.0.0'
    # port = 5000
    httpd = simple_server.make_server(host, port, app)
    # print("Serving on %s %d" % (host, port))
    httpd.serve_forever()
