#[cfg(test)]
mod airlock_tests;

#[cfg(test)]
mod production_tests;

#[cfg(test)]
mod stress_tests;

// Only include integration tests if postgres feature is enabled
#[cfg(all(test, feature = "postgres-tests"))]
mod integration_tests;