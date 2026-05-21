# AWS Lambda public image for Python 3.11.
FROM public.ecr.aws/lambda/python:3.11

# pyarrow is the heavy dependency that pushes this into Container Image
# territory (>50MB zipped). Pin to a known-good version compatible with
# the Lambda runtime.
RUN pip install --no-cache-dir \
    "pyarrow==15.0.2" \
    "boto3==1.34.69"

# Lambda task root is /var/task. Copy handler in.
COPY handler.py ${LAMBDA_TASK_ROOT}/

# Entry point handler module.function
CMD ["handler.handler"]
