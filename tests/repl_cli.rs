use assert_cmd::prelude::*;
use assert_fs::{prelude::*, NamedTempFile};
use predicates::prelude::*;
use std::{
    io::Write,
    process::{Command, Stdio},
};

fn test_cmd(temp_file: &NamedTempFile) -> Result<std::process::Child, Box<dyn std::error::Error>> {
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
fn inserts_data() -> Result<(), Box<dyn std::error::Error>> {
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
fn persists_data() -> Result<(), Box<dyn std::error::Error>> {
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

    Ok(())
}
