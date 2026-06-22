<a href='https://github.com/Junwu0615/Platform Genesis'><img alt='GitHub Views' src='https://views.whatilearened.today/views/github/Junwu0615/Platform Genesis.svg'>
[![Back to HomePage](https://img.shields.io/badge/%F0%9F%8C%90_Back_to-HomePage-blue?style=flat-square)](https://github.com/Junwu0615/Platform-Genesis)

## *⭐ PG-Shared-Lib ⭐*

<br>

### *A.　Implement*

<details>
<summary><b><i>　Tree </i></b></summary>
<ul>

```bash
tree -I 'venv|.git|__pycache__|docs|logs|assets|kafka_data|charts'

.
├── .pre-commit-config.yaml
├── LICENSE
├── README.md
├── requirements.txt
├── setup.cfg
├── setup.py
├── shared
│   ├── __init__.py
│   ├── configs
│   │   ├── __init__.py
│   │   ├── constant.py
│   │   └── settings.py
│   ├── modules
│   │   ├── __init__.py
│   │   ├── entry.py
│   │   ├── kafka_consumer.py
│   │   ├── kafka_producer.py
│   │   ├── log.py
│   │   └── mqtt.py
│   └── utils
│       ├── __init__.py
│       ├── env_config.py
│       ├── postgres_tools.py
│       └── tools.py
└── shared.egg-info
    ├── PKG-INFO
    ├── SOURCES.txt
    ├── dependency_links.txt
    ├── requires.txt
    └── top_level.txt
```

</ul>
</details>

<br>

### *B.　Install Packages*
- #### *b.1.　DEV*
    ```
    pip install -e .
    ```

- #### *b.2.　Container*
    ```
    # requirements.txt
    git+https://github.com/Junwu0615/PG-Shared-Lib@main
    ```

<br>

### *C.　Push Code*
- #### *c.1.　Manual → Not Recommended*
    ```bash
    # <語法格式版本>
    black --version
    # <語法格式檢查>
    black src/
    
    # 期望輸出
    # All done! ✨ 🍰 ✨
    # ?? files left unchanged.
    ```

- #### *c.2.　Auto → Recommended*
    ```bash
    # 全域設定 ( 一次性 )
        # 1. 透過 Ubuntu 系統套件管理員安裝 pipx
        sudo apt update && sudo apt install -y pipx
        
        # 2. pipx 自動配置環境變數路徑
        pipx ensurepath
        
        # 3. 用 pipx 安裝 pre-commit
        pipx install pre-commit
    
    # ⭐ 當前專案的 .pre-commit-config.yaml & pre-commit 工具正式綁定
    pre-commit install
    
    # ⭐ 強制檢查
    pre-commit run --all-files
    ```

<br><br><br>
