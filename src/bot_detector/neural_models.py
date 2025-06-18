import torch
import torch.nn as nn
from .paths import OPEN_MODEL, CLOSE_MODEL

torch.set_num_threads(8)


class DataNormalizer(torch.nn.Module):
    def __init__(self, is_close: bool, device: str):
        super().__init__()
        self.device = device

        if is_close:
            mc_max = [1, 1, 1, 1, 1, 1, 1, 1, 648, 1, 764, 1, 28, 1, 261, 1]
        else:
            mc_max = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 10, 1,
                      835, 1, 716, 1, 1095, 1, 1173, 1, 426, 1, 56, 1, 545, 1, 1, 1, 1714, 1, 62, 1]
        self.max_clip = torch.tensor(mc_max, dtype=torch.float32, device=self.device)
        self.min_clip = torch.zeros_like(self.max_clip)

    def forward(self, item):
        tensor = torch.tensor(item, dtype=torch.float32, device=self.device)
        clip_tensor = tensor.clip(self.min_clip, self.max_clip)
        return (clip_tensor - self.min_clip) / (self.max_clip - self.min_clip)


class PredictionModel:
    def __init__(self, is_close):
        self.model: nn.Sequential | None = None
        self.input_size = 16 if is_close else 45
        self.is_close = is_close
        self.transform = DataNormalizer(is_close, 'cpu')

        self.load_model_from_params()

    def define_model(self):
        self.model = nn.Sequential(
            nn.Linear(self.input_size, 128),
            nn.ReLU(),
            nn.Dropout(0.4),

            nn.Linear(128, 64),
            nn.ReLU(),
            nn.Dropout(0.4),

            nn.Linear(64, 1),
            nn.Sigmoid()
        )

    def load_model_from_params(self):
        self.define_model()
        model_params = torch.load(
            CLOSE_MODEL if self.is_close else OPEN_MODEL,
            weights_only=True
        )
        self.model.load_state_dict(model_params)
        self.model.eval()

    def model_predict(self, profile_data_list: list):
        with torch.no_grad():
            results = []
            for profile_data in profile_data_list:
                prediction = self.model(self.transform(profile_data[1]))
                prediction = torch.clip(prediction, 0, 1)
                results.append((profile_data[0], round(prediction.item(), 4)))
        return results
