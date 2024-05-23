# FROM python:3.9 

# WORKDIR /app 

# COPY requirements.txt .
# RUN pip install --no-cache-dir --upgrade pip && \
#     pip install --no-cache-dir -r requirements.txt



# CMD ["/app/.venv/bin/python", "kafka1:/home/appuser/Producer.py"]
FROM python:3.9

# Création de l'environnement virtuel
RUN python -m venv /app/.venv

# Configuration du répertoire de travail
WORKDIR /app

# Copie du fichier Python
COPY Producer.py /app

# Installation des dépendances
COPY requirements.txt .
RUN /app/.venv/bin/pip install --no-cache-dir --upgrade pip && \
    /app/.venv/bin/pip install --no-cache-dir -r requirements.txt

# Spécification de la commande à exécuter
CMD ["/app/.venv/bin/python", "Producer.py"]
