package com.beifeng.model;

/**
 * 对应数据库的model类
 * 
 * @author gerry
 *
 */
public class User implements java.io.Serializable {

	private static final long serialVersionUID = 1101160720279300694L;
	private Integer id;
	private String name;
	private Integer age;

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getAge() {
		return age;
	}

	public void setAge(Integer age) {
		this.age = age;
	}
}
