"""
Import as:

import optimizer.utils as outi
"""

import logging
from typing import Optional

import numpy as np
import pandas as pd

import core.signal_processing as csipro
import helpers.dbg as hdbg

_LOG = logging.getLogger(__name__)

# TODO(Paul): Write a function to check for PSD.


def is_symmetric(matrix: pd.DataFrame, **kwargs) -> bool:
    m, n = matrix.shape
    hdbg.dassert_eq(m, n)
    return np.allclose(matrix, matrix.T, **kwargs)


def compute_tangency_portfolio(
    mu_rows: pd.DataFrame,
    *,
    covariance: Optional[pd.DataFrame] = None,
    precision: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Compute the Markowitz tangency portfolio adjusted with Kelly leverage.

    Note that the expected SR for each row is given by
        `compute_quadratic_form(mu_rows, precision)`
    where `precision` is the inverse of the covariance matrix.

    :param mu_rows: mean excess returns (along rows)
    :param covariance: covariance matrix
    :param precision: precision matrix, i.e., the inverse of the covariance
        matrix
    :return: rows of weights
    """
    if covariance is not None:
        hdbg.dassert(
            precision is None,
            "Exactly one of `covariance` and `precision` must be not `None`.",
        )
        hdbg.dassert(is_symmetric(covariance))
        precision = csipro.compute_pseudoinverse(covariance, hermitian=True)
    else:
        hdbg.dassert(
            precision is not None,
            "Exactly one of `covariance` and `precision` must be not `None`.",
        )
        hdbg.dassert(is_symmetric(precision))
    weights = mu_rows.dot(precision)
    return weights


def compute_quadratic_form(
    row_vectors: pd.DataFrame, symmetric_matrix: pd.DataFrame
) -> pd.DataFrame:
    """
    Given a fixed real symmetric_matrix matrix `M`, compute `v.T A v` row-wise.

    :param row_vectors: numerical dataframe
    :param symmetric_matrix: a real symmetric matrix (aligned with columns of
        `row_vectors`)
    :return: quadratic form evaluated on each row vector
    """
    hdbg.dassert(is_symmetric(symmetric_matrix))
    # For each row `v` of `df`, compute `v.T * symmetric_matrix * v`.
    quadratic = row_vectors.multiply(row_vectors.dot(symmetric_matrix)).sum(
        axis=1
    )
    return quadratic


def neutralize(row_vectors: pd.DataFrame) -> pd.DataFrame:
    """
    Orthogonally project row vectors onto subspace orthogonal to all ones.

    (In other words, we demean each row).

    If the rows of `row_vectors` represent dollar positions, then this operation
    will dollar neutralize each row.

    :param row_vectors: numerical dataframe
    :return: neutralized `row_vectors` (i.e., demeaned)
    """
    mean = row_vectors.mean(axis=1)
    projection = row_vectors.subtract(mean, axis=0)
    return projection
