package com.training.hadoop.custom.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Writable;

public class CitiesBidCountWritable implements Writable {
	private Set<Integer> cities = new HashSet<Integer>();

	private int count;

	public CitiesBidCountWritable() {
	}

	public CitiesBidCountWritable(Set<Integer> cities, int count) {
		this.cities.addAll(cities);
		this.count = count;
	}

	public CitiesBidCountWritable(int city, int count) {
		cities.add(city);
		this.count = count;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(cities.size());
		for (int city : cities) {
			out.writeInt(city);
		}
		out.writeInt(count);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		for (int i = 0; i < size; i++) {
			cities.add(in.readInt());
		}
		count = in.readInt();
	}

	@Override
	public String toString() {
		return Arrays.toString(cities.toArray()) + ":" + Integer.toString(count);
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof CitiesBidCountWritable))
			return false;
		
		CitiesBidCountWritable other = (CitiesBidCountWritable) o;

		if (this.count != other.count)
			return false;
		
		if (this.cities.size() != other.cities.size())
			return false;
		
		ArrayList<Integer> otherCities = new ArrayList<Integer> (other.cities);
		return !otherCities.retainAll(this.cities);
	}
	
	public Set<Integer> getCities() {
		return cities;
	}

	public void setCities(Set<Integer> city) {
		this.cities = city;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

}
