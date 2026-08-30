import kagglehub
import shutil
from pathlib import Path
# Download latest version
path = Path(kagglehub.dataset_download("marcodena/mobile-phone-activity"))

dest_path = Path("D:\\phase_1_project")

shutil.copytree(path, dest_path, dirs_exist_ok=True)

print(dest_path.resolve())
