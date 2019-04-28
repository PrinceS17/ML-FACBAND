from mrtparse import *
import datetime
import calendar
import time

class FeatureExtractor:
	def __init__(self, path, name):
		self.path = path
		self.features = {}
		self.name = name
		self.ts = -1
		self.fm = {
			"num_anouncement": 1, 
			"num_withdrawn": 2,
			"num_nlris": 3,
			"num_wnlris": 4,
			"avg_aspath": 5,
			"max_aspath": 6,
			"avg_uniq_aspath": 7,
			"num_dup": 8,
			"num_dup_withdrawn": 9,
			"num_imp": 10,
			"avg_ed": 11,
			"max_ed": 12,
			"inter_arrival": 13,
			"ed7": 14,
			"ed8": 15,
			"ed9": 16,
			"ed10": 17,
			"ed11": 18,
			"ed12": 19,
			"ed13": 20,
			"ed14": 21,
			"ed15": 22,
			"ed16": 23,
			"ed17": 24,
			"as7": 25,
			"as8": 26,
			"as9": 27,
			"as10": 28,
			"as11": 29,
			"as12": 30,
			"as13": 31,
			"as14": 32,
			"as15": 33,
			"num_igp": 34,
			"num_egp": 35,
			"num_incomplete": 36,
			"packet_size": 37,
			"ts": 38
		}
	
	def ed(self, as1, as2):
		as1 = as1.split()
		as2 = as2.split()
		m, n = len(as1), len(as2)
		dp = [[0 for x in range(n + 1)] for y in range(m + 1)]
		for i in range(m + 1):
		    for j in range(n + 1):
		        if i == 0:
		            dp[i][j] = j
		        elif j == 0:
		            dp[i][j] = i
		        elif as1[i - 1] == as2[j - 1]:
		            dp[i][j] = dp[i - 1][j - 1]
		        else:
		            dp[i][j] = 1 + min(dp[i][j - 1], dp[i - 1][j], dp[i - 1][j - 1])
		return dp[m][n]

	# Feature 1-4, 8-10
	def i2iv_viii2x(self, mrt):
		msg = mrt.bgp.msg
		if msg.wd_len == 0:
			for nlri in msg.nlri:
				nlri_s = nlri.prefix + "/" + str(nlri.plen)
				if nlri_s not in self.features["nlris"]:
					self.features["nlris"][nlri_s] = 0
					self.features["num_nlris"] += 1	
				self.features["nlris"][nlri_s] += 1
			batch = " ".join(sorted(self.features["nlris"].keys()))
			if batch not in self.features["unlri"]:
				self.features["unlri"].add(batch)
				self.features["num_anouncement"] += 1
			else:
				for attr in msg.attr:
					if attr.type == BGP_ATTR_T['AS_PATH']:
						for aspath in attr.as_path:
							aspath_s = " ".join(aspath["val"])
							if aspath_s not in self.features["aspaths"]:
								self.features["num_imp"] += 1
							else:
								self.features["num_dup"] += 1
		else:
			for withdrawn in msg.withdrawn:
				withdrawn_s = withdrawn.prefix + "/" + str(withdrawn.plen)
				if withdrawn_s not in self.features["wnlris"]:
					self.features["wnlris"][withdrawn_s] = 0
					self.features["num_wnlris"] += 1
				self.features["wnlris"][withdrawn_s] += 1
			batch = " ".join(sorted(self.features["wnlris"].keys()))
			if batch not in self.features["uwnlri"]:
				self.features["uwnlri"].add(batch)
				self.features["num_withdrawn"] += 1
			else:
				self.features["num_dup_withdrawn"] += 1

	# Feature 5 & 6 & 7
	def v2vii(self, mrt):
		for attr in mrt.bgp.msg.attr:
			if attr.type == BGP_ATTR_T['AS_PATH']:
				for aspath in attr.as_path:
					aspath_s = " ".join(aspath["val"])
					if aspath_s not in self.features["aspaths"]:
						self.features["aspaths"][aspath_s] = 0
						tp = self.features["avg_uniq_aspath"]
						self.features["avg_uniq_aspath"] = (tp[0] + 1, tp[1] + aspath["len"])
					self.features["aspaths"][aspath_s] += 1
					tp = self.features["avg_aspath"]
					self.features["avg_aspath"] = (tp[0] + 1, tp[1] + aspath["len"])
					self.features["max_aspath"] = max(self.features["max_aspath"], aspath["len"])	

	# Feature 11 & 12
	def xi2xii(self, mrt):
		peer_as = mrt.bgp.peer_as
		for attr in mrt.bgp.msg.attr:
			if attr.type == BGP_ATTR_T['AS_PATH']:
				for aspath in attr.as_path:
					aspath_s = " ".join(aspath["val"])
					if peer_as in self.features["peers2as"]:
						tp = self.features["avg_ed"]
						self.features["avg_ed"] = (tp[0] + 1, self.ed(self.features["peers2as"][peer_as], aspath_s))
						self.features["max_ed"] = max(self.features["max_ed"], self.ed(self.features["peers2as"][peer_as], aspath_s))
					self.features["peers2as"][peer_as] = aspath_s

	# Feature 13
	def inter_arrival(self, mrt):
		ts = int(mrt.ts / 60)
		tp = self.features["inter_arrival"]
		if len(tp) < 3:
			self.features["inter_arrival"] = (0, 0, mrt.ts)
		else:
			self.features["inter_arrival"] = (tp[0] + 1, tp[1] + max(mrt.ts - tp[2], 0), mrt.ts)

	# Feature 34 & 35 & 36
	def num_igp_egp_incomplete(self, mrt):
		for attr in mrt.bgp.msg.attr:
			if attr.type == BGP_ATTR_T['ORIGIN']:
				if attr.origin == 0:
					self.features["num_igp"] += 1
				if attr.origin == 1:
					self.features["num_egp"] += 1
				if attr.origin == 2:
					self.features["num_incomplete"] += 1

	# Feature 37
	def packet_size(self, mrt):
		tp = self.features["packet_size"]
		self.features["packet_size"] = (tp[0] + 1, tp[1] + mrt.bgp.msg.len)

	def end(self):
		# Feature 5
		tp = self.features["avg_aspath"]
		if tp[0] == 0:
			self.features["avg_aspath"] = 0
		else:
			self.features["avg_aspath"] = tp[1] / tp[0]
		# Feature 7
		tp = self.features["avg_uniq_aspath"]
		if tp[0] == 0:
			self.features["avg_uniq_aspath"] = 0
		else:	
			self.features["avg_uniq_aspath"] = tp[1] / tp[0]
		# Feature 11
		tp = self.features["avg_ed"]
		if tp[0] == 0:
			self.features["avg_ed"] = 0
		else:	
			self.features["avg_ed"] = tp[1] / tp[0]
		# Feature 13
		tp = self.features["inter_arrival"]
		if tp[0] == 0:
			self.features["inter_arrival"] = 0
		else:	
			self.features["inter_arrival"] = tp[1] / tp[0]
		# Feature 14-24
		if "ed{0}".format(self.features["max_ed"]) in self.features:
			self.features["ed{0}".format(self.features["max_ed"])] = True
		# Feature 25-33
		if "as{0}".format(self.features["max_aspath"]) in self.features:
			self.features["as{0}".format(self.features["max_aspath"])] = True
		# Feature 37
		tp = self.features["packet_size"]
		if tp[0] == 0:
			self.features["packet_size"] = 0
		else:	
			self.features["packet_size"] = tp[1] / tp[0]

		self.features = {
			"num_anouncement": self.features["num_anouncement"], 
			"num_withdrawn": self.features["num_withdrawn"],
			"num_nlris": self.features["num_nlris"],
			"num_wnlris": self.features["num_wnlris"],
			"avg_aspath": self.features["avg_aspath"],
			"max_aspath": self.features["max_aspath"],
			"avg_uniq_aspath": self.features["avg_uniq_aspath"],
			"num_dup": self.features["num_dup"],
			"num_dup_withdrawn": self.features["num_dup_withdrawn"],
			"num_imp": self.features["num_imp"],
			"avg_ed": self.features["avg_ed"],
			"max_ed": self.features["max_ed"],
			"inter_arrival": self.features["inter_arrival"],
			"ed7": self.features["ed7"],
			"ed8": self.features["ed8"],
			"ed9": self.features["ed9"],
			"ed10": self.features["ed10"],
			"ed11": self.features["ed11"],
			"ed12": self.features["ed12"],
			"ed13": self.features["ed13"],
			"ed14": self.features["ed14"],
			"ed15": self.features["ed15"],
			"ed16": self.features["ed16"],
			"ed17": self.features["ed17"],
			"as7": self.features["as7"],
			"as8": self.features["as8"],
			"as9": self.features["as9"],
			"as10": self.features["as10"],
			"as11": self.features["as11"],
			"as12": self.features["as12"],
			"as13": self.features["as13"],
			"as14": self.features["as14"],
			"as15": self.features["as15"],
			"num_igp": self.features["num_igp"],
			"num_egp": self.features["num_egp"],
			"num_incomplete": self.features["num_incomplete"],
			"packet_size": self.features["packet_size"],
			"ts": self.ts
		}

	def refresh(self):
		self.features = {
			"num_anouncement": 0,
			"num_withdrawn": 0,
			"num_nlris": 0,
			"num_wnlris": 0,
			"avg_aspath": (0, 0),
			"max_aspath": 0,
			"avg_uniq_aspath": (0, 0),
			"num_dup": 0,
			"num_dup_withdrawn": 0,
			"num_imp": 0,
			"avg_ed": (0, 0),
			"max_ed": 0,
			"inter_arrival": (0, 0),
			"ed7": False,
			"ed8": False,
			"ed9": False,
			"ed10": False,
			"ed11": False,
			"ed12": False,
			"ed13": False,
			"ed14": False,
			"ed15": False,
			"ed16": False,
			"ed17": False,
			"as7": False,
			"as8": False,
			"as9": False,
			"as10": False,
			"as11": False,
			"as12": False,
			"as13": False,
			"as14": False,
			"as15": False,
			"num_igp": 0,
			"num_egp": 0,
			"num_incomplete": 0,
			"packet_size": (0, 0),
			"nlris": {},
			"unlri": set(),
			"wnlris": {},
			"uwnlri": set(),
			"aspaths": {},
			"peers2as": {}
		}

	def process(self, start_time, end_time):
		for p in self.path:
			print("processing file", p)
			for packet in Reader(p):
				if packet.mrt and packet.mrt.bgp and packet.mrt.bgp.msg and packet.mrt.bgp.msg.type == BGP_MSG_T['UPDATE']:
					mrt = packet.mrt
					ts = int(mrt.ts / 60)
					if ts != self.ts:
						if self.ts > 0:
							self.end()
							with open("dataset/{0}.arff".format(self.name), "a") as f:
								line = [str(y[1]) for y in sorted(self.features.items(), key=lambda x: self.fm[x[0]])]
								line.append("Regular" if self.ts < start_time or self.ts > end_time else "Anomaly")
								f.write(",".join(line) + "\n")
						self.ts = ts
						self.refresh()
					self.i2iv_viii2x(mrt)
					self.v2vii(mrt)
					self.xi2xii(mrt)
					self.inter_arrival(mrt)
					self.num_igp_egp_incomplete(mrt)
					self.packet_size(mrt)

	def header(self):
		with open("{0}.arff".format(self.name), "w") as f:
			f.write("@RELATION {0}\n".format(self.name))
			f.write("@ATTRIBUTE num_anouncement NUMERIC\n")
			f.write("@ATTRIBUTE num_withdrawn NUMERIC\n")
			f.write("@ATTRIBUTE num_nlris NUMERIC\n")
			f.write("@ATTRIBUTE num_wnlris NUMERIC\n")
			f.write("@ATTRIBUTE avg_aspath NUMERIC\n")
			f.write("@ATTRIBUTE max_aspath NUMERIC\n")
			f.write("@ATTRIBUTE avg_uniq_aspath NUMERIC\n")
			f.write("@ATTRIBUTE num_dup NUMERIC\n")
			f.write("@ATTRIBUTE num_dup_withdrawn NUMERIC\n")
			f.write("@ATTRIBUTE num_imp NUMERIC\n")
			f.write("@ATTRIBUTE avg_ed NUMERIC\n")
			f.write("@ATTRIBUTE max_ed NUMERIC\n")
			f.write("@ATTRIBUTE inter_arrival NUMERIC\n")
			f.write("@ATTRIBUTE ed7 {True,False}\n")
			f.write("@ATTRIBUTE ed8 {True,False}\n")
			f.write("@ATTRIBUTE ed9 {True,False}\n")
			f.write("@ATTRIBUTE ed10 {True,False}\n")
			f.write("@ATTRIBUTE ed11 {True,False}\n")
			f.write("@ATTRIBUTE ed12 {True,False}\n")
			f.write("@ATTRIBUTE ed13 {True,False}\n")
			f.write("@ATTRIBUTE ed14 {True,False}\n")
			f.write("@ATTRIBUTE ed15 {True,False}\n")
			f.write("@ATTRIBUTE ed16 {True,False}\n")
			f.write("@ATTRIBUTE ed17 {True,False}\n")
			f.write("@ATTRIBUTE as7 {True,False}\n")
			f.write("@ATTRIBUTE as8 {True,False}\n")
			f.write("@ATTRIBUTE as9 {True,False}\n")
			f.write("@ATTRIBUTE as10 {True,False}\n")
			f.write("@ATTRIBUTE as11 {True,False}\n")
			f.write("@ATTRIBUTE as12 {True,False}\n")
			f.write("@ATTRIBUTE as13 {True,False}\n")
			f.write("@ATTRIBUTE as14 {True,False}\n")
			f.write("@ATTRIBUTE as15 {True,False}\n")
			f.write("@ATTRIBUTE num_igp NUMERIC\n")
			f.write("@ATTRIBUTE num_egp NUMERIC\n")
			f.write("@ATTRIBUTE num_incomplete NUMERIC\n")
			f.write("@ATTRIBUTE packet_size NUMERIC\n")
			f.write("@ATTRIBUTE ts NUMERIC\n")
			f.write("@ATTRIBUTE class {Regular,Anomaly}\n")
			f.write("@data\n")

def main():
	extractor = FeatureExtractor(sys.argv[1:-2], "slammer")
	extractor.header()

	pattern = '%Y-%m-%d-%H-%M-%S'
	start_time = int(calendar.timegm(time.strptime(sys.argv[-2], pattern))) // 60
	end_time = int(calendar.timegm(time.strptime(sys.argv[-1], pattern))) // 60

	extractor.process(start_time, end_time)

if __name__ == "__main__":
	main()
