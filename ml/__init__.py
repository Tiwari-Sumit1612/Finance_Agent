# from ml.predictor import PredictionResult, SignalPredictor

# __all__ = [
#     "PredictionResult",
#     "SignalPredictor",
# ]
import torch

print(torch.cuda.is_available())
print(torch.cuda.device_count())
if torch.cuda.is_available():
    print(torch.cuda.get_device_name(0))