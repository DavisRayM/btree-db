use assert_cmd::prelude::*;
use assert_fs::{prelude::*, NamedTempFile};
use predicates::prelude::*;
use std::{
    io::Write,
    process::{Command, Stdio},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn test_cmd(temp_file: &NamedTempFile) -> Result<std::process::Child> {
    let cmd = Command::cargo_bin("btree-db")?
        .arg("-f")
        .arg(temp_file.path())
        .stdin(Stdio::piped())
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    Ok(cmd)
}

#[test]
fn inserts_data() -> Result<()> {
    let file = assert_fs::NamedTempFile::new("temp.db")?;
    file.touch()?;
    let mut cmd = test_cmd(&file)?;

    cmd.stdin
        .as_mut()
        .unwrap()
        .write_all(b"insert 1 hello world!\n")?;
    cmd.stdin.as_mut().unwrap().write_all(b"select\n")?;
    cmd.stdin.as_mut().unwrap().write_all(b".exit\n")?;

    cmd.wait_with_output()?
        .assert()
        .success()
        .stdout(predicate::str::contains("hello world!"));
    file.close()?;
    Ok(())
}

#[test]
fn persists_data() -> Result<()> {
    let file = assert_fs::NamedTempFile::new("temp.db")?;
    file.touch()?;
    let mut cmd = test_cmd(&file)?;

    for i in 0..3 {
        cmd.stdin
            .as_mut()
            .unwrap()
            .write_all(format!("insert {i} {i}data\n").as_bytes())?;
    }
    cmd.stdin.as_mut().unwrap().write_all(b".exit\n")?;
    cmd.wait_with_output()?.assert().success();

    let mut cmd = test_cmd(&file)?;
    cmd.stdin.as_mut().unwrap().write_all(b"select\n")?;
    cmd.stdin.as_mut().unwrap().write_all(b".exit\n")?;

    cmd.wait_with_output()?
        .assert()
        .success()
        .stdout(predicate::str::contains("1data"))
        .stdout(predicate::str::contains("2data"))
        .stdout(predicate::str::contains("2data"));

    file.close()?;
    Ok(())
}

#[test]
fn duplicate_keys_rejected() -> Result<()> {
    let file = assert_fs::NamedTempFile::new("temp.db")?;
    file.touch()?;
    let mut cmd = test_cmd(&file)?;

    cmd.stdin
        .as_mut()
        .unwrap()
        .write_all(b"insert 1 some data\n")?;
    cmd.stdin
        .as_mut()
        .unwrap()
        .write_all(b"insert 1 some modified data\n")?;
    cmd.stdin.as_mut().unwrap().write_all(b"select\n")?;
    cmd.stdin.as_mut().unwrap().write_all(b".exit\n")?;

    cmd.wait_with_output()?
        .assert()
        .success()
        .stdout(predicate::str::contains("some data"))
        .stdout(predicate::str::contains("some modified data").not())
        .stdout(predicate::str::contains("error: duplicate key"));

    file.close()?;
    Ok(())
}
