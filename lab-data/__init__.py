import azure.functions as func
import logging

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_body()
        logging.info(f"Raw request body length: {len(body)} bytes")

        return func.HttpResponse("OK")
    except Exception as e:
        logging.exception("Error parsing request:")
        return func.HttpResponse("Failed", status_code=500)
