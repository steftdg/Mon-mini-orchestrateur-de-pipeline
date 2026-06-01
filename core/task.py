from enum import Enum
from typing import Callable, List, Optional
import time


class TaskStatus(Enum):
    """Les états possibles d'une tâche."""
    PENDING  = "pending"   # en attente, pas encore lancée
    RUNNING  = "running"   # en cours d'exécution
    SUCCESS  = "success"   # terminée avec succès
    FAILED   = "failed"    # échouée définitivement
    SKIPPED  = "skipped"   # ignorée (ex: dépendance en échec)


class Task:
    """
    Une tâche = une fonction Python + des métadonnées.

    Exemple d'utilisation :
        def fetch_data():
            print("fetching...")

        t = Task(name="fetch", fn=fetch_data, retries=3)
        t.run()
    """

    def __init__(
        self,
        name: str,
        fn: Callable,
        dependencies: Optional[List[str]] = None,
        retries: int = 0,
        retry_delay: float = 1.0,
    ):
        # Identifiant unique de la tâche dans le DAG
        self.name = name

        # La vraie fonction à exécuter
        self.fn = fn

        # Noms des tâches qui doivent finir avant celle-ci
        self.dependencies = dependencies or []

        # Combien de fois réessayer en cas d'échec
        self.retries = retries

        # Secondes à attendre entre deux essais
        self.retry_delay = retry_delay

        # État courant — commence toujours en attente
        self.status = TaskStatus.PENDING

        # Résultat retourné par fn(), ou None
        self.result = None

        # Message d'erreur si ça plante, ou None
        self.error: Optional[str] = None

        # Timestamps pour mesurer la durée
        self.started_at: Optional[float] = None
        self.finished_at: Optional[float] = None

    # ------------------------------------------------------------------
    # Propriété calculée : durée d'exécution en secondes
    # ------------------------------------------------------------------
    @property
    def duration(self) -> Optional[float]:
        if self.started_at and self.finished_at:
            return round(self.finished_at - self.started_at, 3)
        return None

    # ------------------------------------------------------------------
    # Méthode principale : exécuter la tâche avec retry logic
    # ------------------------------------------------------------------
    def run(self) -> bool:
        """
        Lance fn() avec retry logic.
        Retourne True si succès, False si échec définitif.
        """
        self.status = TaskStatus.RUNNING
        self.started_at = time.time()

        attempts = self.retries + 1  # 1 essai normal + N retries

        for attempt in range(1, attempts + 1):
            try:
                self.result = self.fn()
                # Succès — on sort immédiatement
                self.status = TaskStatus.SUCCESS
                self.finished_at = time.time()
                return True

            except Exception as e:
                self.error = str(e)

                if attempt < attempts:
                    # Il reste des essais — on attend et on réessaie
                    print(f"  [RETRY] {self.name} — essai {attempt}/{attempts - 1} échoué : {e}")
                    time.sleep(self.retry_delay)
                else:
                    # Plus d'essais disponibles — échec définitif
                    self.status = TaskStatus.FAILED
                    self.finished_at = time.time()
                    return False

    # ------------------------------------------------------------------
    # Représentation lisible pour le debug
    # ------------------------------------------------------------------
    def __repr__(self) -> str:
        deps = f" (dépend de: {self.dependencies})" if self.dependencies else ""
        return f"Task({self.name!r}, status={self.status.value}{deps})"