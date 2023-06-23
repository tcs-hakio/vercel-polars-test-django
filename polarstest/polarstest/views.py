from django.http import HttpResponse
from polarstest.utils import measure_performance_time, read_parquet_from_gcs, write_parquet_local


def process_parquet(request):
    df = measure_performance_time(read_parquet_from_gcs)
    measure_performance_time(lambda: write_parquet_local(df))
    return HttpResponse("Parquet processing completed.")
