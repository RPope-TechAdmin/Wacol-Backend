import azure.functions as func
import logging

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Function triggered.")

    try:
        return func.HttpResponse("✅ Function ran successfully.", status_code=200)
    except Exception as e:
        logging.exception("❌ Error occurred:")
        return func.HttpResponse(f"❌ {str(e)}", status_code=500)
