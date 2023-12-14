from flask import Flask, request, jsonify
import pymongo
from pymongo import MongoClient

#start API service
app = Flask(__name__) 

# connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['Occupational_Employment_and_Wage_Statistics']
collection = db['data_collection']


@app.route('/api/data', methods=['GET'])
def get_data():
    # get data from API
    items = list(collection.find({},{'_id':0}))
    # return in JSON format
    return jsonify(items)    


@app.route('/api/data_area_employment', methods=['GET'])
def get_data_area_employment():
    # define pipeline
    pipeline = [
        {
            '$group': {
                '_id': '$Area',
                'total_employment': {
                    '$sum': '$Employment'
                }
            }
        },
        {
            '$sort': {
                'total_employment': -1
            }
        }
    ]
    # aggregate pipeline
    result = collection.aggregate(pipeline)
    return jsonify(list(result))   


@app.route('/api/data_by_occupation', methods=['GET'])
def get_data_by_occupation():
    occupation_title = request.args.get('occupation_title')
    order_by = request.args.get('order_by', 'Median Wage')
    
    sort_column = None
    if order_by == 'Experienced Wage':
        sort_column = 'Experienced Wage'
    elif order_by == 'Median Wage':
        sort_column = 'Median Wage'
    elif order_by == 'Entry Wage':
        sort_column = 'Entry Wage'
    
    query = {'Occupational Title': occupation_title}
    
    if sort_column:
        items = list(collection.find(query, {'_id': 0}).sort(sort_column, pymongo.DESCENDING))
    else:
        # Handle cases where an invalid sorting column is provided
        return jsonify({'error': 'Invalid sorting column provided'}), 400
    
    
    return jsonify(items)



@app.route('/api/median_wage_statistics', methods=['GET'])
def get_median_wage_statistics():
    area_or_occupation = request.args.get('area_or_occupation')
    
    if area_or_occupation == 'area':
        # Provide median wage statistics by area
        pipeline = [
            {
                '$group': {
                    '_id': '$Area',
                    'median_wage': {
                        '$avg': '$Median Wage'
                    }
                }
            },
            {
                '$sort': {
                    'median_wage': -1
                }
            }
        ]
    else:
        # Provide median wage statistics by occupation
        pipeline = [
            {
                '$group': {
                    '_id': '$Occupational Title',
                    'median_wage': {
                        '$avg': '$Median Wage'
                    }
                }
            },
            {
                '$sort': {
                    'median_wage': -1
                }
            }
        ]
    
    result = collection.aggregate(pipeline)
    return jsonify(list(result))


# run Flask API
if __name__ == '__main__':
    app.run(debug=True)

