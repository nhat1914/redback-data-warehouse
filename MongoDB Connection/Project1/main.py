from app import app
from app.controllers import document_controller  # Import controllers to register routes

@app.route('/')
def home():
    return 'Welcome to my world'
  
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
