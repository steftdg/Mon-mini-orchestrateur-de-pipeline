# Mon-mini-orchestrateur-de-pipeline
Un scheduler maison qui exécute des tâches DAG-style, avec retry logic, logs structurés et alertes


#Architecture

mon_orchestrateur/
│
├── core/              ← le cerveau
│   ├── dag.py         # logique du graphe, topological sort
│   ├── task.py        # définition d'une tâche
│   └── runner.py      # exécution, retry, timeout
│
├── scheduler/         ← le timer
│   └── scheduler.py   # cron, triggers
│
├── storage/           ← la mémoire
│   ├── models.py      # tables SQLAlchemy
│   └── db.py          # connexion, migrations
│
├── api/               ← la porte d'entrée
│   └── routes.py      # FastAPI endpoints
│
├── cli/               ← interface terminal
│   └── main.py        # commandes typer/click
│
├── ui/                ← interface visuelle
│   └── templates/     # HTML Jinja2
│
├── examples/          ← IMPORTANT pour GitHub
│   └── etl_pipeline.py # un vrai exemple qui tourne
│
├── tests/             ← indispensable pour crédibilité
│   └── test_dag.py
│
├── README.md
└── requirements.txt
