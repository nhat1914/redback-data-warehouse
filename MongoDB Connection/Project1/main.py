import os
from app import app
from app.controllers import document_controller  # Import controllers to register routes

@app.route('/')
def home():
    return 'Welcome to my world'

if __name__ == '__main__':
    # Use an environment variable to control debug mode
    debug_mode = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
    app.run(debug=debug_mode, host='0.0.0.0')