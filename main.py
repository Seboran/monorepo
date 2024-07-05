import scipy.optimize as opt
import numpy as np


def solve_for_n(balances: list):
    n = len(balances)

    def constraint1(vec):
        # Transform 9x1 vector into 3x3 matrix
        vec = np.reshape(vec, (n, n))
        # Define a second vector
        vecteur_uns = np.ones((n))

        # Compute the dot product of vector1 and vector2
        product = np.dot(vec, vecteur_uns)

        return (product - np.array(balances))

    def constraint_anti_symmetric(vec):
        # Reshape vec into a 3x3 matrix
        matrix = np.reshape(vec, (n, n))
        # Calculate the difference between matrix and its negative transpose
        diff = matrix + matrix.T
        # Flatten the difference matrix to a vector and return its L2 norm
        return np.linalg.norm(diff.flatten(), ord=2)

    def minimize_vector(vector1):
        # Return the result
        return np.sum(np.abs(vector1))

    # Use scipy.optimize.minimize to find the minimum of the function
    somme_initiale_a_transferer = sum(
        filter(lambda x: x > 0, BALANCES_6_PERSONNES))
    initial_guess = np.zeros(n**2)

    print("somme_initiale_a_transferer", somme_initiale_a_transferer)
    result = opt.minimize(minimize_vector, x0=initial_guess, constraints=[
        {'type': 'eq', 'fun': constraint1},  # First constraint
        # Second constraint ensuring diagonal terms are zero
        {'type': 'eq', 'fun': constraint_anti_symmetric},
    ], options={'disp': True, 'maxiter': 1000})

    argument_result = np.reshape(result.x, (n, n))
    argument_result = np.round(argument_result, 2)
    # Print the minimum value and the corresponding argument
    print("Minimum value:", result.fun / 2)
    print("Argument that minimizes the function:\n", argument_result)


def ajouter_depense(depense: int, index_receveur: int, balances: list):
    nombre_rembourseurs = len(balances)
    nouvelles_balances = [balance - depense /
                          nombre_rembourseurs for balance in balances]
    nouvelles_balances[index_receveur] += depense
    return nouvelles_balances


BALANCES_6_PERSONNES = np.zeros(5)
for count in np.random.randint(1, 200, 150):
    BALANCES_6_PERSONNES = ajouter_depense(
        count, np.random.randint(0, len(BALANCES_6_PERSONNES)), BALANCES_6_PERSONNES)

print(BALANCES_6_PERSONNES)
print("somme totale", sum(filter(lambda x: x > 0, BALANCES_6_PERSONNES)))

solve_for_n(BALANCES_6_PERSONNES)
