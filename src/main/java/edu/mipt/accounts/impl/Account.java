package edu.mipt.accounts.impl;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Version;
import lombok.Data;

@Data
@Entity
public class Account {
    @Version
    private long version;
    @Id
    private long id;
    private long balance;

    public Account() {
    }

    public Account(long id, long balance) {
        this.id = id;
        this.balance = balance;
    }

    public void withdraw(long amount) {
        if (balance < amount) throw new AccountException(balance);
        balance -= amount;
    }

    public void deposit(long amount) {
        balance += amount;
    }
}