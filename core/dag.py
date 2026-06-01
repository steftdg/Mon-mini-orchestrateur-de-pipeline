from collections import deque
from typing import Dict, List
from core.task import Task, TaskStatus


class DAG:
    """
    Directed Acyclic Graph — graphe de dépendances entre tâches.

    Exemple d'utilisation :
        dag = DAG(name="mon_pipeline")
        dag.add_task(Task("download", fn=download))
        dag.add_task(Task("clean",    fn=clean,    dependencies=["download"]))
        dag.add_task(Task("save",     fn=save,     dependencies=["clean"]))

        order = dag.resolve()  # ["download", "clean", "save"]
    """

    def __init__(self, name: str):
        self.name = name

        # Dictionnaire nom -> objet Task
        # C'est le registre central de toutes les tâches du pipeline
        self.tasks: Dict[str, Task] = {}

    # ------------------------------------------------------------------
    # Ajouter une tâche au DAG
    # ------------------------------------------------------------------
    def add_task(self, task: Task) -> "DAG":
        """
        Enregistre une tâche.
        Retourne self pour pouvoir chaîner : dag.add_task(t1).add_task(t2)
        """
        if task.name in self.tasks:
            raise ValueError(f"Une tâche nommée '{task.name}' existe déjà dans ce DAG.")

        self.tasks[task.name] = task
        return self

    # ------------------------------------------------------------------
    # Valider que les dépendances déclarées existent bien
    # ------------------------------------------------------------------
    def _validate(self) -> None:
        """
        Vérifie que chaque dépendance déclarée correspond à une tâche réelle.
        Ex : si clean dépend de "downlod" (faute de frappe), on le détecte ici.
        """
        for task in self.tasks.values():
            for dep_name in task.dependencies:
                if dep_name not in self.tasks:
                    raise ValueError(
                        f"La tâche '{task.name}' dépend de '{dep_name}' "
                        f"qui n'existe pas dans le DAG '{self.name}'."
                    )

    # ------------------------------------------------------------------
    # Algorithme de Kahn — tri topologique
    # ------------------------------------------------------------------
    def resolve(self) -> List[str]:
        """
        Calcule l'ordre d'exécution des tâches en respectant les dépendances.
        Retourne une liste ordonnée de noms de tâches.
        Lève une exception si un cycle est détecté.

        Algorithme de Kahn (1962) — logique :
          1. Compte combien de dépendances chaque tâche a encore (in-degree)
          2. Mets en file les tâches avec in-degree = 0 (prêtes à tourner)
          3. Prends une tâche de la file, ajoute-la à l'ordre final
          4. Pour chaque tâche qui dépendait d'elle, décrémente son in-degree
          5. Si son in-degree devient 0, elle est prête — ajoute-la à la file
          6. Recommence jusqu'à vider la file
          7. S'il reste des tâches non traitées → cycle détecté
        """
        self._validate()

        # Étape 1 — calculer le nombre de dépendances restantes par tâche
        # in_degree["save"] = 2 signifie que save attend 2 tâches avant de pouvoir démarrer
        in_degree: Dict[str, int] = {name: 0 for name in self.tasks}

        for task in self.tasks.values():
            for dep in task.dependencies:
                in_degree[task.name] += 1

        # Étape 2 — file de départ : tâches sans aucune dépendance
        # deque = double-ended queue, plus efficace que list pour pop gauche
        ready: deque = deque(
            name for name, degree in in_degree.items() if degree == 0
        )

        execution_order: List[str] = []

        # Étape 3 à 6 — vider la file en propageant
        while ready:
            # On prend la première tâche prête
            current = ready.popleft()
            execution_order.append(current)

            # Pour chaque tâche qui dépend de current,
            # on lui enlève une dépendance — current vient de finir
            for task in self.tasks.values():
                if current in task.dependencies:
                    in_degree[task.name] -= 1

                    # Cette tâche n'attend plus personne — elle est prête
                    if in_degree[task.name] == 0:
                        ready.append(task.name)

        # Étape 7 — si on n'a pas traité toutes les tâches, il y a un cycle
        if len(execution_order) != len(self.tasks):
            # Identifier les tâches coincées pour aider au debug
            stuck = set(self.tasks) - set(execution_order)
            raise ValueError(
                f"Cycle détecté dans le DAG '{self.name}'. "
                f"Tâches bloquées : {stuck}"
            )

        return execution_order

    # ------------------------------------------------------------------
    # Visualiser le DAG dans le terminal
    # ------------------------------------------------------------------
    def describe(self) -> None:
        """Affiche un résumé lisible du pipeline et de ses dépendances."""
        print(f"\nDAG : {self.name}")
        print(f"  {len(self.tasks)} tâche(s)\n")

        try:
            order = self.resolve()
            for i, name in enumerate(order, 1):
                task = self.tasks[name]
                deps = f" ← {task.dependencies}" if task.dependencies else ""
                status = task.status.value
                print(f"  {i}. {name}{deps}  [{status}]")
        except ValueError as e:
            print(f"  ERREUR : {e}")

    def __repr__(self) -> str:
        return f"DAG({self.name!r}, tasks={list(self.tasks.keys())})"