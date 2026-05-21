FROM public.ecr.aws/lambda/python:3.11

# Quarantine helper is small (no pyarrow); could be zipfile-deployed,
# but staying with Container Image for consistency with the validator
# Lambda — same build pipeline, same base image.
RUN pip install --no-cache-dir "boto3==1.34.69"

COPY handler.py ${LAMBDA_TASK_ROOT}/

CMD ["handler.handler"]
